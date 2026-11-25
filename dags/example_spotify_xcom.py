"""
## Spotify Playlist Top 5 - Ejemplo de XCom con API real

Este DAG enseña cómo funciona XCom en Airflow usando la API real de Spotify
para ordenar las canciones de una playlist propia por reproducciones recientes.

### ¿Qué es XCom?

XCom (cross-communication) es como una "memoria compartida" entre tareas.
Cuando una tarea retorna un valor, Airflow lo guarda automáticamente.
La siguiente tarea puede leerlo y usarlo.

### Flujo:

```
fetch_top_songs  →  rank_top_songs  →  show_daily_summary
```

- **fetch_top_songs**: Obtiene todas las canciones de la playlist y cuenta
  cuántas veces aparece cada una entre hasta 50 reproducciones recientes.
  El `return` guarda los datos en XCom automáticamente.
- **rank_top_songs**: Recibe esos datos (Airflow los inyecta como parámetro),
  ordena por número de reproducciones y devuelve las primeras cinco.
- **show_daily_summary**: Recibe el top 5 y muestra el resumen.

### Requisitos previos

1. Crea una app en https://developer.spotify.com/dashboard.
2. Escribe el Client ID y Client Secret en las constantes de este archivo.
3. Ejecuta `scripts/spotify_authorize.ps1` en Windows o
   `scripts/spotify_authorize.py` en macOS/Linux para autorizar tu cuenta.
   El asistente guarda el nuevo refresh token en `.env` y en este DAG.

El DAG renueva un token de usuario en cada ejecución para leer la playlist.
Spotify no devuelve el total histórico de reproducciones de cada canción:
este ranking solo cuenta hasta 50 escuchas recientes de la cuenta autorizada,
independientemente de desde dónde se hayan reproducido.

### Documentación oficial de Airflow

- [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Conceptos: Tasks](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
"""

import logging
from collections import Counter

import requests
from airflow.sdk import dag, task
from pendulum import datetime, duration

# --------------- #
# DAG Constants   #
# --------------- #

# Reemplaza estos valores con los de tu app de Spotify.
SPOTIFY_CLIENT_ID = "TU_CLIENT_ID"
SPOTIFY_CLIENT_SECRET = "TU_CLIENT_SECRET"
SPOTIFY_REFRESH_TOKEN = "TU_REFRESH_TOKEN"

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

# Playlist propia "2026"
TOP_HITS_PLAYLIST_ID = "2MqzwRYIcPgpuRmYQ0gzYs"

TOP_N = 5
HTTP_TIMEOUT_SECONDS = 15
PLAYLIST_PAGE_SIZE = 50
RECENT_PLAYS_LIMIT = 50

log = logging.getLogger(__name__)


# --------------- #
# DAG Definition  #
# --------------- #


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=3,
    doc_md=__doc__,
    default_args={
        "owner": "Universidad",
        "retries": 2,
        "retry_delay": duration(seconds=5),
    },
    tags=["example", "educativo", "spotify", "xcom"],
    is_paused_upon_creation=True,  # El DAG se crea pausado; hay que activarlo manualmente en la UI
    catchup=False,  # No ejecuta runs pasados; solo programa desde ahora en adelante
)
def example_spotify_xcom():
    """Pipeline lineal que demuestra XCom: fetch → rank → summary."""

    @task
    def fetch_top_songs() -> list[dict]:
        """
        Obtiene canciones de la playlist y cuenta sus reproducciones recientes.

        Usa el refresh token obtenido tras autorizar al propietario.

        XCOM: Al hacer ``return``, Airflow guarda este valor
        automáticamente para que otras tareas lo lean.
        Puedes verlo en la UI: click en la tarea → XCom.
        """
        # Paso 1: Renovar el token de acceso del usuario
        if any(
            not value or value.startswith("TU_")
            for value in (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REFRESH_TOKEN)
        ):
            raise RuntimeError("Completa las tres credenciales de Spotify en este DAG.")

        token_response = requests.post(
            SPOTIFY_TOKEN_URL,
            data={"grant_type": "refresh_token", "refresh_token": SPOTIFY_REFRESH_TOKEN},
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        if token_response.status_code == 400:
            try:
                spotify_error = token_response.json().get("error")
            except ValueError:
                spotify_error = None
            if spotify_error == "invalid_grant":
                raise RuntimeError(
                    "El refresh token ha caducado o no es válido. "
                    "Ejecuta scripts/spotify_authorize.ps1 (Windows) o "
                    "scripts/spotify_authorize.py (macOS/Linux) para autorizar de nuevo."
                )
        if token_response.status_code == 401:
            raise RuntimeError("Spotify rechazó el Client ID o Client Secret configurados en el DAG.")
        token_response.raise_for_status()
        access_token: str = token_response.json().get("access_token", "")
        if not access_token:
            raise RuntimeError("Spotify no ha devuelto un access token.")

        # Paso 2: Obtener todas las canciones de la playlist, página a página
        playlist_url = f"{SPOTIFY_API_BASE_URL}/playlists/{TOP_HITS_PLAYLIST_ID}/items"
        headers = {"Authorization": f"Bearer {access_token}"}
        songs: list[dict] = []
        seen_track_ids: set[str] = set()
        offset = 0
        while True:
            playlist_response = requests.get(
                playlist_url,
                headers=headers,
                params={"limit": PLAYLIST_PAGE_SIZE, "offset": offset},
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            playlist_response.raise_for_status()
            page = playlist_response.json()
            items: list[dict] = page.get("items", [])
            for entry in items:
                track: dict = entry.get("item") or {}
                track_id = track.get("id")
                if (
                    not track_id
                    or track_id in seen_track_ids
                    or track.get("type") not in (None, "track")
                ):
                    continue
                seen_track_ids.add(track_id)
                artists_list: list[dict] = track.get("artists") or []
                artist_name = (
                    artists_list[0].get("name", "Desconocido")
                    if artists_list else "Desconocido"
                )
                songs.append({
                    "id": track_id,
                    "title": track.get("name", "Sin título"),
                    "artist": artist_name,
                    "album": (track.get("album") or {}).get("name", "Sin álbum"),
                })
            offset += len(items)
            if not items or offset >= page.get("total", 0):
                break

        # Paso 3: Contar eventos recientes por ID de canción (no por título).
        recent_response = requests.get(
            f"{SPOTIFY_API_BASE_URL}/me/player/recently-played",
            headers=headers,
            params={"limit": RECENT_PLAYS_LIMIT},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        recent_response.raise_for_status()
        recent_items = recent_response.json().get("items", [])
        plays_by_track = Counter(
            entry["track"]["id"] for entry in recent_items
            if entry.get("track") and entry["track"].get("id")
        )
        for song in songs:
            song["plays"] = plays_by_track[song["id"]]

        log.info(
            "Obtenidas %d canciones y %d reproducciones recientes.",
            len(songs), len(recent_items),
        )
        return songs  # ← Esto se guarda en XCom

    @task
    def rank_top_songs(all_songs: list[dict]) -> list[dict]:
        """
        Devuelve las cinco canciones con más reproducciones recientes.

        XCOM: El parámetro ``all_songs`` viene automáticamente de
        la tarea anterior. Airflow lo lee de XCom por ti
        porque usamos TaskFlow API (@task).
        """
        ranked_songs: list[dict] = sorted(
            all_songs, key=lambda song: song["plays"], reverse=True
        )[:TOP_N]

        if ranked_songs and ranked_songs[0]["plays"] == 0:
            log.warning("Ninguna canción de la playlist aparece entre las reproducciones recientes.")
        log.info("Top %d por reproducciones recientes:", TOP_N)
        for rank, song in enumerate(ranked_songs, start=1):
            log.info(
                "  #%d %s - %s (%d reproducciones)",
                rank, song["title"], song["artist"], song["plays"],
            )
        return ranked_songs  # ← Esto también se guarda en XCom

    @task
    def show_daily_summary(top_songs: list[dict]) -> None:
        """
        Muestra el resumen final del top 5.

        XCOM: Recibe el top 5 de la tarea anterior.

        Esta tarea no retorna nada, así que no guarda XCom.
        """
        if not top_songs:
            log.warning("No hay canciones para mostrar.")
            return

        number_one: dict = top_songs[0]

        log.info("=" * 50)
        log.info("TOP %d DE LA PLAYLIST EN HASTA %d ESCUCHAS", TOP_N, RECENT_PLAYS_LIMIT)
        log.info("=" * 50)
        for rank, song in enumerate(top_songs, start=1):
            log.info(
                "  #%d %s - %s [%s] (%d reproducciones)",
                rank,
                song.get("title", "?"),
                song.get("artist", "?"),
                song.get("album", "?"),
                song.get("plays", 0),
            )
        log.info("-" * 50)
        log.info(
            "Primera canción: %s de %s",
            number_one.get("title", "N/A"), number_one.get("artist", "N/A"),
        )
        log.info("=" * 50)

    # --- Dependencias: cada llamada pasa el resultado a la siguiente ---
    # Airflow usa XCom internamente para mover los datos entre tareas.
    all_songs = fetch_top_songs()
    top_songs = rank_top_songs(all_songs)       # all_songs viene de XCom
    show_daily_summary(top_songs)               # top_songs viene de XCom


example_spotify_xcom()
