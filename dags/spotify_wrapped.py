r"""
## Spotify Wrapped: API → XCom → PostgreSQL

Guarda las reproducciones que Spotify entrega en "Recently Played" y evita
duplicados al ejecutarse cada 30 minutos. El dashboard consulta los últimos
365 días **registrados**. La API no garantiza el historial completo anterior
a la primera ejecución; para completarlo se necesita el export de Spotify.

Una sola orden desde la raíz del proyecto crea PostgreSQL, configura la
conexión `spotify_postgres` en Airflow, ejecuta este DAG y abre el dashboard:

* Windows PowerShell: `powershell -ExecutionPolicy Bypass -File .\scripts\setup_spotify_windows.ps1`
* Ubuntu: `bash scripts/setup_spotify_ubuntu.sh`
* macOS: `bash scripts/setup_spotify_macos.sh`

Antes de arrancar por primera vez, autoriza tu cuenta con
`scripts/spotify_authorize.ps1` (Windows) para obtener el permiso
`user-read-recently-played`, o suministra las tres variables
`SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, `SPOTIFY_REFRESH_TOKEN` en `.env`.
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task


CONN_ID = "spotify_postgres"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_RECENT_URL = "https://api.spotify.com/v1/me/player/recently-played"
log = logging.getLogger(__name__)


@dag(
    dag_id="spotify_wrapped",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="*/30 * * * *",
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["spotify", "postgres", "xcom", "curso"],
    doc_md=__doc__,
)
def spotify_wrapped():
    create_table = SQLExecuteQueryOperator(
        task_id="create_spotify_tables",
        conn_id=CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS spotify_plays (
            played_at TIMESTAMPTZ NOT NULL,
            track_id TEXT NOT NULL,
            track_uri TEXT,
            track_name TEXT NOT NULL,
            artist_name TEXT NOT NULL,
            album_name TEXT,
            album_image_url TEXT,
            duration_ms INTEGER,
            spotify_url TEXT,
            context_uri TEXT,
            source TEXT NOT NULL DEFAULT 'api',
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (played_at, track_id)
        );
        ALTER TABLE spotify_plays
            ADD COLUMN IF NOT EXISTS album_image_url TEXT;
        CREATE INDEX IF NOT EXISTS spotify_plays_played_at_idx
            ON spotify_plays (played_at DESC);
        CREATE INDEX IF NOT EXISTS spotify_plays_artist_name_idx
            ON spotify_plays (artist_name);
        """,
        split_statements=True,
        do_xcom_push=False,
    )

    @task
    def fetch_recent_plays() -> list[dict]:
        """Devuelve hasta 50 reproducciones de la API en un XCom de Airflow."""
        client_id = os.environ.get("SPOTIFY_CLIENT_ID")
        client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
        refresh_token = os.environ.get("SPOTIFY_REFRESH_TOKEN")
        if not all((client_id, client_secret, refresh_token)):
            raise RuntimeError("Faltan credenciales de Spotify en el .env de Astro.")

        token_response = requests.post(
            SPOTIFY_TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            auth=(client_id, client_secret),
            timeout=15,
        )
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]

        year_ago_ms = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000)
        response = requests.get(
            SPOTIFY_RECENT_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 50, "after": year_ago_ms},
            timeout=15,
        )
        response.raise_for_status()

        plays = []
        for entry in response.json().get("items", []):
            track = entry.get("track") or {}
            track_id = track.get("id") or track.get("uri")
            played_at = entry.get("played_at")
            if not track_id or not played_at:
                continue
            artists = track.get("artists") or []
            album = track.get("album") or {}
            images = album.get("images") or []
            plays.append(
                {
                    "played_at": played_at,
                    "track_id": track_id,
                    "track_uri": track.get("uri"),
                    "track_name": track.get("name") or "Sin título",
                    "artist_name": artists[0].get("name", "Desconocido") if artists else "Desconocido",
                    "album_name": album.get("name"),
                    "album_image_url": images[0].get("url") if images else None,
                    "duration_ms": track.get("duration_ms"),
                    "spotify_url": (track.get("external_urls") or {}).get("spotify"),
                    "context_uri": (entry.get("context") or {}).get("uri"),
                }
            )
        log.info("Spotify entregó %d reproducciones; XCom las pasa a PostgreSQL.", len(plays))
        return plays

    extracted = fetch_recent_plays()

    load_plays = SQLExecuteQueryOperator(
        task_id="load_spotify_plays",
        conn_id=CONN_ID,
        sql="""
        INSERT INTO spotify_plays (
            played_at, track_id, track_uri, track_name, artist_name,
            album_name, album_image_url, duration_ms, spotify_url, context_uri
        )
        SELECT p.played_at::timestamptz, p.track_id, p.track_uri, p.track_name,
               p.artist_name, p.album_name, p.album_image_url, p.duration_ms,
               p.spotify_url, p.context_uri
        FROM jsonb_to_recordset(%(plays)s::jsonb) AS p (
            played_at TEXT, track_id TEXT, track_uri TEXT, track_name TEXT,
            artist_name TEXT, album_name TEXT, album_image_url TEXT,
            duration_ms INTEGER,
            spotify_url TEXT, context_uri TEXT
        )
        ON CONFLICT (played_at, track_id) DO UPDATE SET
            track_uri = EXCLUDED.track_uri,
            track_name = EXCLUDED.track_name,
            artist_name = EXCLUDED.artist_name,
            album_name = EXCLUDED.album_name,
            album_image_url = EXCLUDED.album_image_url,
            duration_ms = EXCLUDED.duration_ms,
            spotify_url = EXCLUDED.spotify_url,
            context_uri = EXCLUDED.context_uri;
        """,
        parameters={"plays": "{{ ti.xcom_pull(task_ids='fetch_recent_plays') | tojson }}"},
        do_xcom_push=False,
    )

    create_table >> extracted >> load_plays


spotify_wrapped()
