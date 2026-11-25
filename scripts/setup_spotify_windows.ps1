param([switch]$NoBrowser)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Invoke-Checked([string]$label, [scriptblock]$command) {
    & $command
    if ($LASTEXITCODE -ne 0) { throw "$label ha fallado (código $LASTEXITCODE)." }
}

function Read-EnvValue([string]$name) {
    foreach ($line in [IO.File]::ReadAllLines((Join-Path $root '.env'))) {
        if ($line.StartsWith("$name=")) { return $line.Substring($name.Length + 1) }
    }
    return ''
}

foreach ($command in @('docker', 'astro')) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "Falta $command. Instala Docker Desktop y Astro CLI (winget install -e --id Astronomer.Astro)."
    }
}
Invoke-Checked 'Docker Desktop' { docker info --format '{{.ServerVersion}}' | Out-Null }
if (-not (Test-Path -LiteralPath '.env')) { throw 'Falta .env con SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET.' }
if (-not (Read-EnvValue 'SPOTIFY_CLIENT_ID') -or -not (Read-EnvValue 'SPOTIFY_CLIENT_SECRET')) {
    throw 'Completa SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET en .env.'
}

$needsAuthorization = -not (Read-EnvValue 'SPOTIFY_REFRESH_TOKEN')
if (-not $needsAuthorization) {
    $clientId = Read-EnvValue 'SPOTIFY_CLIENT_ID'
    $clientSecret = Read-EnvValue 'SPOTIFY_CLIENT_SECRET'
    $refreshToken = Read-EnvValue 'SPOTIFY_REFRESH_TOKEN'
    $basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${clientId}:${clientSecret}"))
    try {
        $tokenResponse = Invoke-RestMethod -Uri 'https://accounts.spotify.com/api/token' -Method Post -Headers @{ Authorization = "Basic $basic" } -ContentType 'application/x-www-form-urlencoded' -Body @{ grant_type = 'refresh_token'; refresh_token = $refreshToken } -TimeoutSec 15
        Invoke-RestMethod -Uri 'https://api.spotify.com/v1/me/player/recently-played?limit=1' -Headers @{ Authorization = "Bearer $($tokenResponse.access_token)" } -TimeoutSec 15 | Out-Null
    } catch {
        $code = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
        if ($code -eq 401 -or $code -eq 403 -or $code -eq 400) { $needsAuthorization = $true }
        else { throw "No se pudo verificar Spotify (HTTP $code)." }
    }
}
if ($needsAuthorization) {
    Write-Output 'Spotify necesita el permiso user-read-recently-played. Abriendo la autorización...'
    & (Join-Path $PSScriptRoot 'spotify_authorize.ps1') -TimeoutSeconds 600 -NoPlaylistCheck
    if ($LASTEXITCODE -ne 0) { throw 'La autorización de Spotify no se completó.' }
}

$password = Read-EnvValue 'SPOTIFY_DB_PASSWORD'
if (-not $password) {
    $bytes = New-Object byte[] 24
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $password = ([BitConverter]::ToString($bytes)).Replace('-', '').ToLowerInvariant()
    $envPath = Join-Path $root '.env'
    $lines = @([IO.File]::ReadAllLines($envPath) | Where-Object { -not $_.StartsWith('SPOTIFY_DB_PASSWORD=') })
    $lines += "SPOTIFY_DB_PASSWORD=$password"
    [IO.File]::WriteAllLines($envPath, [string[]]$lines, [Text.UTF8Encoding]::new($false))
}

docker network inspect spotify_history_net *> $null
if ($LASTEXITCODE -ne 0) {
    Invoke-Checked 'Red compartida de Spotify' { docker network create spotify_history_net | Out-Null }
}
Invoke-Checked 'Docker Compose' { docker compose -f compose.spotify.yml up -d --build --wait }
$status = astro dev ps --json | ConvertFrom-Json
$schedulerRunning = @($status.containers | Where-Object { $_.state -eq 'running' -and $_.name -match '-scheduler-\d+$' }).Count -gt 0
if ($schedulerRunning) {
    Invoke-Checked 'Reinicio de Airflow' { astro dev restart }
} else {
    Invoke-Checked 'Arranque de Airflow' { astro dev start --no-browser }
}

$status = astro dev ps --json | ConvertFrom-Json
foreach ($container in $status.containers) {
    if ($container.state -ne 'running' -or $container.name -notmatch '-(api-server|scheduler|dag-processor|triggerer)-\d+$') { continue }
    $networks = docker inspect --format '{{json .NetworkSettings.Networks}}' $container.name | ConvertFrom-Json
    if (-not $networks.PSObject.Properties['spotify_history_net']) {
        Invoke-Checked 'Conexión de red Docker' { docker network connect spotify_history_net $container.name }
    }
}

$yaml = "airflow:`n  connections:`n    - conn_id: spotify_postgres`n      conn_type: postgres`n      conn_host: spotify-db`n      conn_schema: spotify_history`n      conn_login: spotify`n      conn_password: $password`n      conn_port: 5432`n"
[IO.File]::WriteAllText((Join-Path $root 'spotify_airflow_settings.yaml'), $yaml, [Text.UTF8Encoding]::new($false))
Invoke-Checked 'Conexión PostgreSQL en Airflow' { astro dev object import --connections --settings-file spotify_airflow_settings.yaml }

$logicalDate = Get-Date -Format 'yyyy-MM-dd'
Invoke-Checked 'DAG spotify_wrapped' { astro dev run dags test spotify_wrapped $logicalDate -f /usr/local/airflow/dags/spotify_wrapped.py }
Invoke-Checked 'Activación del DAG' { astro dev run dags unpause spotify_wrapped }
$health = Invoke-WebRequest -Uri 'http://127.0.0.1:8501/_stcore/health' -UseBasicParsing -TimeoutSec 15
if ($health.StatusCode -ne 200) { throw 'El dashboard no respondió correctamente.' }
Write-Output 'PostgreSQL, conexión Airflow, DAG y dashboard están listos.'
Write-Output 'Dashboard: http://127.0.0.1:8501'
if (-not $NoBrowser) { Start-Process 'http://127.0.0.1:8501' }
