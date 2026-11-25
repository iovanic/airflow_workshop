#!/usr/bin/env bash
set -euo pipefail

platform="${1:?Indica ubuntu o macos}"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

if [[ "$platform" == macos ]]; then
  if ! command -v brew >/dev/null 2>&1; then
    echo 'Instala Homebrew antes de continuar: https://brew.sh/' >&2
    exit 1
  fi
  if ! command -v astro >/dev/null 2>&1; then brew install astro; fi
  if ! command -v python3 >/dev/null 2>&1; then brew install python; fi
  if ! command -v docker >/dev/null 2>&1; then brew install --cask docker; fi
  if ! docker info >/dev/null 2>&1; then
    open -a Docker
    for _ in $(seq 1 60); do
      if docker info >/dev/null 2>&1; then break; fi
      sleep 2
    done
  fi
else
  if ! command -v docker >/dev/null 2>&1; then
    echo 'Instala Docker Engine y Compose: https://docs.docker.com/engine/install/ubuntu/' >&2
    exit 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo 'Instala Python 3: sudo apt-get install python3' >&2
    exit 1
  fi
  if ! command -v astro >/dev/null 2>&1; then
    echo 'Instala Astro CLI: curl -fsSL https://install.astronomer.io | bash -s' >&2
    exit 1
  fi
  if ! docker info >/dev/null 2>&1 && command -v systemctl >/dev/null 2>&1; then
    sudo systemctl start docker
  fi
fi

docker info >/dev/null || { echo 'Docker no está en marcha o falta permiso de acceso.' >&2; exit 1; }
docker compose version >/dev/null || { echo 'Falta Docker Compose v2.' >&2; exit 1; }
if [[ ! -f .env ]]; then cp .env.example .env; fi
grep -Eq '^SPOTIFY_CLIENT_ID=.+$' .env || { echo 'Completa SPOTIFY_CLIENT_ID en .env.' >&2; exit 1; }
grep -Eq '^SPOTIFY_CLIENT_SECRET=.+$' .env || { echo 'Completa SPOTIFY_CLIENT_SECRET en .env.' >&2; exit 1; }

if ! python3 scripts/spotify_authorize.py --check; then
  python3 scripts/spotify_authorize.py --env-only
fi

password="$(sed -n 's/^SPOTIFY_DB_PASSWORD=//p' .env | head -n 1)"
if [[ -z "$password" ]]; then
  password="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
  if grep -q '^SPOTIFY_DB_PASSWORD=' .env; then
    sed -i.bak "s/^SPOTIFY_DB_PASSWORD=.*/SPOTIFY_DB_PASSWORD=$password/" .env
    rm -f .env.bak
  else
    printf '\nSPOTIFY_DB_PASSWORD=%s\n' "$password" >> .env
  fi
fi

docker network inspect spotify_history_net >/dev/null 2>&1 || docker network create spotify_history_net >/dev/null
docker compose -f compose.spotify.yml up -d --build --wait
if astro dev ps --json | grep -A 1 '"name": ".*-scheduler-' | grep -q '"state": "running"'; then
  astro dev restart
else
  astro dev start --no-browser
fi

while IFS= read -r container; do
  case "$container" in
    *-api-server-*|*-scheduler-*|*-dag-processor-*|*-triggerer-*)
      if ! docker inspect --format '{{json .NetworkSettings.Networks}}' "$container" | grep -q '"spotify_history_net"'; then
        docker network connect spotify_history_net "$container"
      fi
      ;;
  esac
done < <(astro dev ps --json | awk -F'"' '/"name":/ {print $4}')

cat > spotify_airflow_settings.yaml <<EOF
airflow:
  connections:
    - conn_id: spotify_postgres
      conn_type: postgres
      conn_host: spotify-db
      conn_schema: spotify_history
      conn_login: spotify
      conn_password: $password
      conn_port: 5432
EOF
astro dev object import --connections --settings-file spotify_airflow_settings.yaml
astro dev run dags test spotify_wrapped "$(date +%F)" -f /usr/local/airflow/dags/spotify_wrapped.py
astro dev run dags unpause spotify_wrapped
curl -fsS http://127.0.0.1:8501/_stcore/health >/dev/null
echo 'PostgreSQL, conexión Airflow, DAG y dashboard listos: http://127.0.0.1:8501'
if [[ "$platform" == macos ]]; then
  open http://127.0.0.1:8501
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open http://127.0.0.1:8501 >/dev/null 2>&1 || true
fi
