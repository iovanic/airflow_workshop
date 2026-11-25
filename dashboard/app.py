"""Spotify Wrapped: una vista de las reproducciones guardadas desde la API."""

from html import escape
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import streamlit as st


st.set_page_config(page_title="Spotify Wrapped", page_icon="🎧", layout="wide")
MADRID = ZoneInfo("Europe/Madrid")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700;800&family=Space+Grotesk:wght@500;600;700&display=swap');
    html, body, [class*="css"], [data-testid="stApp"] {font-family:'DM Sans',sans-serif;}
    [data-testid="stApp"] {background:#101016;color:#faf7ef;}
    [data-testid="stHeader"] {background:transparent;}
    .block-container {max-width:1450px;padding-top:1.7rem;padding-bottom:4rem;}
    .hero {background:#c3f255;color:#17141f;border-radius:26px;padding:32px 42px 30px;position:relative;overflow:hidden;margin-bottom:18px;}
    .hero:after {content:'✺';position:absolute;right:3%;top:-78px;font-size:250px;line-height:1;color:#7637e9;transform:rotate(-18deg);}
    .eyebrow {font-size:.8rem;letter-spacing:.16em;font-weight:800;text-transform:uppercase;}
    .hero h1 {font-family:'Space Grotesk',sans-serif;font-weight:700;font-size:clamp(3.5rem,8vw,7rem);letter-spacing:-.085em;line-height:.95;margin:16px 0 13px;position:relative;z-index:1;}
    .hero p {font-weight:700;margin:0;position:relative;z-index:1;max-width:720px;}
    .coverage {border:1px solid #5e4c87;background:#241b39;color:#e7dcff;padding:13px 18px;border-radius:15px;margin:0 0 19px;font-size:.88rem;}
    .kpi {border-radius:23px;padding:25px 26px;min-height:150px;color:#17141f;}
    .kpi .label {font-size:.76rem;font-weight:800;letter-spacing:.09em;text-transform:uppercase;}
    .kpi .value {font-family:'Space Grotesk',sans-serif;font-size:clamp(2.3rem,4vw,4.5rem);font-weight:700;line-height:1.1;margin-top:14px;letter-spacing:-.07em;}
    .kpi .foot {font-size:.77rem;font-weight:700;margin-top:5px;opacity:.75;}
    .section-title {font-family:'Space Grotesk',sans-serif;font-size:1.55rem;font-weight:700;letter-spacing:-.04em;margin:23px 0 12px;}
    .spotlight {height:220px;border-radius:23px;padding:25px;display:flex;align-items:flex-end;gap:22px;overflow:hidden;position:relative;}
    .spotlight .copy {position:relative;z-index:2;min-width:0;}
    .spotlight .eyebrow {font-size:.72rem;}
    .spotlight .name {font-family:'Space Grotesk',sans-serif;font-size:clamp(1.5rem,3.4vw,2.9rem);font-weight:700;line-height:1;letter-spacing:-.06em;margin:10px 0 8px;overflow-wrap:anywhere;}
    .spotlight .sub {font-size:.9rem;font-weight:700;opacity:.85;}
    .spotlight img {width:170px;height:170px;object-fit:cover;border-radius:16px;box-shadow:0 15px 40px #0006;flex-shrink:0;position:relative;z-index:1;}
    .spotlight .disc {position:absolute;right:-30px;top:-45px;font-size:260px;line-height:1;opacity:.22;}
    .chart-card {background:#1d1c28;border:1px solid #373247;border-radius:23px;padding:19px 23px;margin-top:8px;min-height:310px;}
    .chart-card h3 {font-family:'Space Grotesk',sans-serif;font-size:1.25rem;margin:0 0 6px;}
    .chart-card p {font-size:.76rem;color:#aaa4bb;margin:0 0 18px;}
    .bar-row {margin:0 0 14px;}
    .bar-label {display:flex;justify-content:space-between;gap:8px;font-size:.88rem;font-weight:700;margin-bottom:6px;}
    .bar-label span:first-child {white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:82%;}
    .bar-track {height:10px;background:#373247;border-radius:20px;overflow:hidden;}
    .bar-fill {height:100%;border-radius:20px;}
    .bottom-note {color:#aaa4bb;font-size:.76rem;margin-top:16px;}
    a.spotify {color:inherit;text-decoration:underline;text-underline-offset:3px;}
    [data-testid="stDataFrame"] {border-radius:18px;overflow:hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60)
def load_plays() -> pd.DataFrame:
    with psycopg.connect(connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT played_at, track_id, track_name, artist_name, album_name,
                          album_image_url, spotify_url
                   FROM spotify_plays
                   WHERE played_at >= NOW() - INTERVAL '365 days'
                   ORDER BY played_at DESC"""
            )
            columns = [col.name for col in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)


def safe_spotify_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = urlparse(value)
    return value if parsed.scheme == "https" and parsed.netloc == "open.spotify.com" else None


def safe_art_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = urlparse(value)
    return value if parsed.scheme == "https" and parsed.netloc in {"i.scdn.co", "mosaic.scdn.co"} else None


def spotlight(label: str, name: str, detail: str, count: int, art: str | None, url: str | None, color: str, ink: str) -> None:
    name_html = escape(str(name))
    if url:
        name_html = f'<a class="spotify" href="{escape(url, quote=True)}" target="_blank" rel="noopener noreferrer">{name_html}</a>'
    image = f'<img src="{escape(art, quote=True)}" alt="Portada del álbum en Spotify">' if art else '<span class="disc">✺</span>'
    st.markdown(
        f'<div class="spotlight" style="background:{color};color:{ink}">'
        f'<div class="copy"><div class="eyebrow">{escape(label)}</div>'
        f'<div class="name">{name_html}</div>'
        f'<div class="sub">{escape(detail)} · {count:,} reproducciones</div></div>{image}</div>',
        unsafe_allow_html=True,
    )


def ranked_card(title: str, description: str, entries: list[tuple[str, int]], color: str) -> None:
    maximum = max((count for _, count in entries), default=1)
    rows = "".join(
        f'<div class="bar-row"><div class="bar-label"><span>{index:02d} &nbsp; {escape(str(name))}</span><span>{count:,}</span></div>'
        f'<div class="bar-track"><div class="bar-fill" style="width:{100 * count / maximum:.1f}%;background:{color}"></div></div></div>'
        for index, (name, count) in enumerate(entries, 1)
    )
    st.markdown(
        f'<div class="chart-card"><h3>{escape(title)}</h3><p>{escape(description)}</p>{rows}</div>',
        unsafe_allow_html=True,
    )


st.markdown(
    '<div class="hero"><div class="eyebrow">Tu música · últimos 365 días registrados</div>'
    '<h1>Spotify Wrapped</h1><p>Una mirada a lo que has escuchado desde que empezó la captura.</p></div>',
    unsafe_allow_html=True,
)

try:
    plays = load_plays()
except (psycopg.Error, OSError) as exc:
    st.warning("Aún no hay datos disponibles. Ejecuta el DAG `spotify_wrapped` en Airflow y actualiza esta página.")
    st.caption(f"Estado de la base de datos: {type(exc).__name__}")
    st.stop()

if plays.empty:
    st.info("Todavía no hay reproducciones. Escucha música en Spotify y ejecuta el DAG `spotify_wrapped`.")
    st.stop()

plays["played_at"] = pd.to_datetime(plays["played_at"], utc=True)
plays["local_time"] = plays["played_at"].dt.tz_convert(MADRID)
plays["local_day"] = plays["local_time"].dt.date
first = plays["local_day"].min()
last = plays["local_day"].max()
first_label = first.strftime("%d/%m/%Y")
last_label = last.strftime("%d/%m/%Y")
st.markdown(
    f'<div class="coverage">📅 Datos observados: <b>{first_label} — {last_label}</b>. '
    'La API solo entrega reproducciones recientes: esta vista crece con cada ejecución del DAG y no reconstruye el año anterior.</div>',
    unsafe_allow_html=True,
)

kpis = [
    ("Reproducciones", len(plays), "registradas por la API", "#c3f255"),
    ("Canciones", plays["track_id"].nunique(), "temas diferentes", "#f8a8de"),
    ("Artistas", plays["artist_name"].nunique(), "artistas principales", "#ab93ff"),
    ("Días activos", plays["local_day"].nunique(), "con actividad registrada", "#ffb37f"),
]
for col, (label, value, foot, color) in zip(st.columns(4, gap="small"), kpis):
    with col:
        st.markdown(
            f'<div class="kpi" style="background:{color}"><div class="label">{label}</div>'
            f'<div class="value">{value:,}</div><div class="foot">{foot}</div></div>',
            unsafe_allow_html=True,
        )

st.markdown('<div class="section-title">En lo más alto ✦</div>', unsafe_allow_html=True)
artist_counts = plays.groupby("artist_name", dropna=False).size().sort_values(ascending=False)
song_counts = plays.groupby(["track_id", "track_name", "artist_name"], dropna=False).size().sort_values(ascending=False)
top_artist = str(artist_counts.index[0])
top_artist_plays = plays[plays["artist_name"] == top_artist]
top_song_id, top_song_name, top_song_artist = song_counts.index[0]
top_song_plays = plays[plays["track_id"] == top_song_id]
left, right = st.columns(2, gap="small")
with left:
    artist_art = next((safe_art_url(v) for v in top_artist_plays["album_image_url"] if safe_art_url(v)), None)
    spotlight("Tu artista más escuchado", top_artist, "Artista nº 1", int(artist_counts.iloc[0]), artist_art, None, "#773df1", "#ffffff")
with right:
    song_art = next((safe_art_url(v) for v in top_song_plays["album_image_url"] if safe_art_url(v)), None)
    song_url = next((safe_spotify_url(v) for v in top_song_plays["spotify_url"] if safe_spotify_url(v)), None)
    spotlight("Tu canción más repetida", str(top_song_name), str(top_song_artist), int(song_counts.iloc[0]), song_art, song_url, "#f8a8de", "#17141f")

left, right = st.columns(2, gap="small")
with left:
    ranked_card("Top artistas", "Por reproducciones registradas", [(str(k), int(v)) for k, v in artist_counts.head(5).items()], "#c3f255")
with right:
    ranked_card("Top canciones", "Por reproducciones registradas", [(str(k[1]), int(v)) for k, v in song_counts.head(5).items()], "#f8a8de")

st.markdown('<div class="section-title">Tu ritmo 🎧</div>', unsafe_allow_html=True)
left, right = st.columns([1.5, 1], gap="large")
with left:
    st.markdown("**Actividad por día**")
    activity = plays.groupby("local_day").size()
    day_index = pd.date_range(first, last, freq="D").date
    activity = activity.reindex(day_index, fill_value=0)
    st.area_chart(activity, color="#c3f255", height=210, x_label="Día", y_label="Reproducciones")
with right:
    st.markdown("**Horas en las que más escuchas**")
    hours = plays.groupby(plays["local_time"].dt.hour).size().reindex(range(24), fill_value=0)
    st.bar_chart(hours, color="#ab93ff", height=210, x_label="Hora local", y_label="Reproducciones")

with st.expander("Ver reproducciones recientes"):
    recent = plays[["local_time", "track_name", "artist_name", "album_name"]].head(100).copy()
    recent["local_time"] = recent["local_time"].dt.strftime("%d/%m/%Y %H:%M")
    recent.columns = ["Fecha y hora", "Canción", "Artista", "Álbum"]
    st.dataframe(recent, hide_index=True, width="stretch")

st.markdown(
    '<div class="bottom-note">Datos de Spotify · Horas en Europe/Madrid · Portadas de Spotify · '
    'Recuento de eventos de Recently Played, no tiempo real de escucha.</div>',
    unsafe_allow_html=True,
)
