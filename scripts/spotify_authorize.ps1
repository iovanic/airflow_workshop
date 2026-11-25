param(
    [int]$TimeoutSeconds = 180,
    [switch]$NoPlaylistCheck
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$dagPath = Join-Path $projectRoot 'dags/example_spotify_xcom.py'
$envPath = Join-Path $projectRoot '.env'
$source = [IO.File]::ReadAllText($dagPath)

function Read-DagConstant([string]$name) {
    $match = [regex]::Match($source, '(?m)^' + [regex]::Escape($name) + '\s*=\s*"([^"]*)"')
    if (-not $match.Success -or -not $match.Groups[1].Value) {
        throw "No se ha encontrado $name en el DAG."
    }
    return $match.Groups[1].Value
}

function Read-EnvValue([string]$name) {
    if (-not (Test-Path -LiteralPath $envPath)) { return '' }
    foreach ($line in [IO.File]::ReadAllLines($envPath)) {
        if ($line.StartsWith("$name=")) { return $line.Substring($name.Length + 1) }
    }
    return ''
}

function Save-EnvValue([string]$name, [string]$value) {
    $lines = if (Test-Path -LiteralPath $envPath) { @([IO.File]::ReadAllLines($envPath)) } else { @() }
    $found = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match "^$name=") {
            $lines[$i] = "$name=$value"
            $found = $true
        }
    }
    if (-not $found) { $lines += "$name=$value" }
    [IO.File]::WriteAllLines($envPath, [string[]]$lines, [Text.UTF8Encoding]::new($false))
}

function Save-DagRefreshToken([string]$value) {
    $quoted = ConvertTo-Json -InputObject $value -Compress
    $pattern = '(?m)^SPOTIFY_REFRESH_TOKEN\s*=\s*"[^"]*"'
    if (-not [regex]::IsMatch($source, $pattern)) {
        throw 'No se ha encontrado SPOTIFY_REFRESH_TOKEN en el DAG.'
    }
    $updated = [regex]::Replace(
        $source,
        $pattern,
        [System.Text.RegularExpressions.MatchEvaluator]{ param($match) "SPOTIFY_REFRESH_TOKEN = $quoted" }
    )
    [IO.File]::WriteAllText($dagPath, $updated, [Text.UTF8Encoding]::new($false))
}

function Send-BrowserResponse($stream, [int]$status, [string]$message) {
    $body = [Text.Encoding]::UTF8.GetBytes($message)
    $reason = if ($status -eq 200) { 'OK' } else { 'Bad Request' }
    $head = [Text.Encoding]::ASCII.GetBytes("HTTP/1.1 $status $reason`r`nContent-Type: text/plain; charset=utf-8`r`nContent-Length: $($body.Length)`r`nConnection: close`r`n`r`n")
    $stream.Write($head, 0, $head.Length)
    $stream.Write($body, 0, $body.Length)
}

$clientId = Read-DagConstant 'SPOTIFY_CLIENT_ID'
$clientSecret = Read-DagConstant 'SPOTIFY_CLIENT_SECRET'
if ($NoPlaylistCheck -or $clientId.StartsWith('TU_') -or $clientSecret.StartsWith('TU_')) {
    $clientId = Read-EnvValue 'SPOTIFY_CLIENT_ID'
    $clientSecret = Read-EnvValue 'SPOTIFY_CLIENT_SECRET'
}
if (-not $clientId -or -not $clientSecret) {
    throw 'Completa SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET en el DAG o en .env.'
}
$playlistId = if ($NoPlaylistCheck) { '' } else { Read-DagConstant 'TOP_HITS_PLAYLIST_ID' }
$redirectUri = 'http://127.0.0.1:8888/callback'
$state = [Guid]::NewGuid().ToString('N')
$authorizeUrl = 'https://accounts.spotify.com/authorize?' +
    'response_type=code' +
    '&client_id=' + [Uri]::EscapeDataString($clientId) +
    '&redirect_uri=' + [Uri]::EscapeDataString($redirectUri) +
    '&scope=' + [Uri]::EscapeDataString('playlist-read-private user-read-recently-played') +
    '&show_dialog=true' +
    '&state=' + [Uri]::EscapeDataString($state)

$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 8888)
$listener.Start()
try {
    Write-Output 'Se abrira Spotify en el navegador. Inicia sesion con tu cuenta y acepta el permiso.'
    Write-Output "Si el navegador no se abre, copia este enlace: $authorizeUrl"
    Start-Process $authorizeUrl

    $acceptTask = $listener.AcceptTcpClientAsync()
    if (-not $acceptTask.Wait($TimeoutSeconds * 1000)) {
        throw 'No se recibio la autorizacion a tiempo. Ejecuta el script de nuevo.'
    }
    $client = $acceptTask.Result
    try {
        $stream = $client.GetStream()
        $stream.ReadTimeout = 5000
        $reader = [IO.StreamReader]::new($stream, [Text.Encoding]::ASCII)
        $requestLine = $reader.ReadLine()
        while ($reader.ReadLine()) { }
        $target = ($requestLine -split ' ')[1]
        $callback = [Uri]::new("http://127.0.0.1:8888$target")
        $query = @{}
        foreach ($part in $callback.Query.TrimStart('?').Split('&')) {
            $pair = $part.Split('=', 2)
            if ($pair.Count -eq 2) {
                $key = [Uri]::UnescapeDataString($pair[0].Replace('+', ' '))
                $value = [Uri]::UnescapeDataString($pair[1].Replace('+', ' '))
                $query[$key] = $value
            }
        }

        if ($callback.AbsolutePath -ne '/callback' -or $query['state'] -ne $state) {
            Send-BrowserResponse $stream 400 'Respuesta de autorizacion no valida.'
            throw 'La respuesta no coincide con la solicitud de autorizacion.'
        }
        if ($query['error']) {
            Send-BrowserResponse $stream 400 'La autorizacion se ha denegado.'
            throw 'Spotify no ha concedido el permiso.'
        }
        if (-not $query['code']) {
            Send-BrowserResponse $stream 400 'Falta el codigo de autorizacion.'
            throw 'Spotify no devolvio un codigo de autorizacion.'
        }

        $basic = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("${clientId}:${clientSecret}"))
        try {
            $tokenResponse = Invoke-RestMethod -Uri 'https://accounts.spotify.com/api/token' -Method Post -Headers @{ Authorization = "Basic $basic" } -ContentType 'application/x-www-form-urlencoded' -Body @{
                grant_type = 'authorization_code'
                code = $query['code']
                redirect_uri = $redirectUri
            } -TimeoutSec 15
            $headers = @{ Authorization = "Bearer $($tokenResponse.access_token)" }
            $user = Invoke-RestMethod -Uri 'https://api.spotify.com/v1/me' -Headers $headers -TimeoutSec 15
            if (-not $NoPlaylistCheck) {
                $playlist = Invoke-RestMethod -Uri "https://api.spotify.com/v1/playlists/$playlistId" -Headers $headers -TimeoutSec 15
                if ($user.id -ne $playlist.owner.id) {
                    throw "La cuenta autorizada no es la propietaria de la playlist '$($playlist.name)'."
                }
                $items = Invoke-RestMethod -Uri "https://api.spotify.com/v1/playlists/$playlistId/items?limit=5" -Headers $headers -TimeoutSec 15
            }
            $recent = Invoke-RestMethod -Uri 'https://api.spotify.com/v1/me/player/recently-played?limit=5' -Headers $headers -TimeoutSec 15
            if (-not $tokenResponse.refresh_token) { throw 'Spotify no devolvio un refresh token.' }
            Save-EnvValue 'SPOTIFY_REFRESH_TOKEN' $tokenResponse.refresh_token
            if (-not $NoPlaylistCheck) {
                Save-DagRefreshToken $tokenResponse.refresh_token
            }
        } catch {
            Send-BrowserResponse $stream 400 'No se ha podido completar la prueba de la API. Consulta la terminal.'
            throw
        }

        Send-BrowserResponse $stream 200 'Autorizacion completada. Ya puedes cerrar esta ventana.'
        Write-Output "Autorizado como $($user.id)."
        Write-Output "Reproducciones recientes accesibles: $(@($recent.items).Count)."
        if (-not $NoPlaylistCheck) {
            Write-Output "Playlist: $($playlist.name). Canciones totales: $($items.total)."
            foreach ($entry in $items.items) {
                $track = if ($entry.item) { $entry.item } else { $entry.track }
                if ($track) { Write-Output "Cancion: $($track.name)" }
            }
        }
        if ($NoPlaylistCheck) {
            Write-Output 'El refresh token se ha guardado en .env sin mostrarlo.'
        } else {
            Write-Output 'El refresh token se ha guardado en .env y en el DAG sin mostrarlo.'
        }
    } finally {
        $client.Close()
    }
} finally {
    $listener.Stop()
}
