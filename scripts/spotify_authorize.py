"""Autoriza Spotify en macOS/Linux y guarda el refresh token en .env y el DAG."""

import base64
import json
import re
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / ".env"
DAG_PATH = ROOT / "dags" / "example_spotify_xcom.py"
REDIRECT_URI = "http://127.0.0.1:8888/callback"


def read_env() -> dict[str, str]:
    if not ENV_PATH.exists():
        return {}
    return dict(line.split("=", 1) for line in ENV_PATH.read_text().splitlines() if "=" in line)


def read_dag_constant(name: str) -> str:
    match = re.search(rf'^{name}\s*=\s*"([^"]*)"', DAG_PATH.read_text(), re.MULTILINE)
    return match.group(1) if match else ""


def save_dag_refresh_token(value: str) -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    updated, count = re.subn(
        r'^SPOTIFY_REFRESH_TOKEN\s*=\s*"[^"]*"',
        lambda _match: f"SPOTIFY_REFRESH_TOKEN = {json.dumps(value)}",
        source,
        count=1,
        flags=re.MULTILINE,
    )
    if count != 1:
        raise RuntimeError("No se ha encontrado SPOTIFY_REFRESH_TOKEN en el DAG")
    DAG_PATH.write_text(updated, encoding="utf-8")


def save_env(name: str, value: str) -> None:
    lines = ENV_PATH.read_text().splitlines() if ENV_PATH.exists() else []
    lines = [line for line in lines if not line.startswith(f"{name}=")]
    lines.append(f"{name}={value}")
    ENV_PATH.write_text("\n".join(lines) + "\n")


def spotify_request(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Spotify devolvió HTTP {error.code} al verificar el permiso") from None


def check_recent_scope() -> None:
    """Termina con código 0 cuando el refresh token actual da acceso al historial."""
    values = read_env()
    client_id = values.get("SPOTIFY_CLIENT_ID", "")
    client_secret = values.get("SPOTIFY_CLIENT_SECRET", "")
    refresh_token = values.get("SPOTIFY_REFRESH_TOKEN", "")
    if not all((client_id, client_secret, refresh_token)):
        raise RuntimeError("Faltan credenciales o refresh token")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    data = urllib.parse.urlencode(
        {"grant_type": "refresh_token", "refresh_token": refresh_token}
    ).encode()
    request = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=data,
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        token = json.load(response)
    spotify_request(
        "https://api.spotify.com/v1/me/player/recently-played?limit=1", token["access_token"]
    )
    if token.get("refresh_token") and token["refresh_token"] != refresh_token:
        save_env("SPOTIFY_REFRESH_TOKEN", token["refresh_token"])


def main(env_only: bool = False) -> None:
    values = read_env()
    client_id = "" if env_only else read_dag_constant("SPOTIFY_CLIENT_ID")
    client_secret = "" if env_only else read_dag_constant("SPOTIFY_CLIENT_SECRET")
    if env_only or not client_id or not client_secret or client_id.startswith("TU_") or client_secret.startswith("TU_"):
        client_id = values.get("SPOTIFY_CLIENT_ID", "")
        client_secret = values.get("SPOTIFY_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        raise RuntimeError("Completa SPOTIFY_CLIENT_ID y SPOTIFY_CLIENT_SECRET en el DAG o en .env")

    state = secrets.token_urlsafe(24)
    authorize_url = "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": REDIRECT_URI,
            "scope": "playlist-read-private user-read-recently-played",
            "state": state,
            "show_dialog": "true",
        }
    )
    result: dict[str, str] = {}

    class Callback(BaseHTTPRequestHandler):
        def log_message(self, *_args) -> None:
            pass  # La URL contiene un código de un solo uso; no registrarla.

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            if parsed.path != "/callback" or query.get("state", [None])[0] != state:
                self.send_error(400, "Respuesta de Spotify no válida")
                return
            result["code"] = query.get("code", [""])[0]
            result["error"] = query.get("error", [""])[0]
            body = "Respuesta recibida. Puedes cerrar esta ventana y volver a la terminal."
            encoded = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    with HTTPServer(("127.0.0.1", 8888), Callback) as server:
        server.timeout = 300
        print("Abre este enlace e inicia sesión con tu cuenta de Spotify:")
        print(authorize_url)
        webbrowser.open(authorize_url)
        server.handle_request()

    if result.get("error"):
        raise RuntimeError("Spotify no ha concedido el permiso")
    if not result.get("code"):
        raise RuntimeError("No se recibió el código de autorización")

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    data = urllib.parse.urlencode(
        {"grant_type": "authorization_code", "code": result["code"], "redirect_uri": REDIRECT_URI}
    ).encode()
    request = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=data,
        headers={"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            token = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Spotify rechazó el intercambio del código: HTTP {error.code}") from None

    spotify_request("https://api.spotify.com/v1/me", token["access_token"])
    recent = spotify_request(
        "https://api.spotify.com/v1/me/player/recently-played?limit=5", token["access_token"]
    )
    save_env("SPOTIFY_REFRESH_TOKEN", token["refresh_token"])
    if not env_only:
        save_dag_refresh_token(token["refresh_token"])
    print(f"Autorización completa. Reproducciones recientes accesibles: {len(recent.get('items', []))}.")
    print("Refresh token guardado en .env" + ("." if env_only else " y en el DAG.") + " No se ha mostrado.")


if __name__ == "__main__":
    if "--check" in sys.argv:
        try:
            check_recent_scope()
            print("Permiso user-read-recently-played verificado.")
        except (RuntimeError, urllib.error.HTTPError) as error:
            print(f"Hay que autorizar Spotify de nuevo: {error}", file=sys.stderr)
            raise SystemExit(1) from None
    else:
        main(env_only="--env-only" in sys.argv)
