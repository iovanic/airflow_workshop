# Curso introductorio de Apache Airflow

> Este repositorio está basado en la plantilla oficial de [Astronomer](https://github.com/astronomer/astro-cli), generada con `astro dev init`. Se ha ampliado con DAGs educativos para un curso universitario de introducción a Airflow.

## Spotify Wrapped: captura y dashboard

El DAG [`spotify_wrapped`](dags/spotify_wrapped.py) consulta cada 30 minutos las reproducciones recientes de la cuenta autorizada. La tarea de extracción entrega los eventos a través de **XCom** y dos tareas `SQLExecuteQueryOperator` crean la tabla y guardan las filas en PostgreSQL sin duplicarlas. El [dashboard](dashboard/app.py) muestra reproducciones, canciones, artistas, días activos, rankings y actividad por día y hora.

### Antes del primer arranque

1. Ten Docker y Astro CLI instalados. En Ubuntu también se necesita Python 3; en macOS, Homebrew. El script de macOS puede instalar Astro CLI, Python y Docker Desktop con Brew si faltan. En Windows comprueba `astro version` y `docker info`.
2. Copia `.env.example` a `.env` y rellena `SPOTIFY_CLIENT_ID` y `SPOTIFY_CLIENT_SECRET` desde tu app de Spotify Developer. No subas `.env` al repositorio.
3. Registra **exactamente** `http://127.0.0.1:8888/callback` como Redirect URI en la app y añade la cuenta de Spotify en **User Management** mientras la app esté en modo desarrollo. La primera ejecución abre el navegador para conceder `user-read-recently-played`; el token se guarda en `.env`.

### Una sola orden desde la raíz del proyecto

| Sistema | Orden |
|---|---|
| Windows PowerShell | `powershell -ExecutionPolicy Bypass -File .\scripts\setup_spotify_windows.ps1` |
| Ubuntu | `bash scripts/setup_spotify_ubuntu.sh` |
| macOS | `bash scripts/setup_spotify_macos.sh` |

El script crea el contenedor `spotify-history-postgres` y el dashboard, configura la conexión **`spotify_postgres`** en Airflow, ejecuta una carga inicial y abre [Spotify Wrapped](http://127.0.0.1:8501). Airflow queda en [localhost:8080](http://localhost:8080). PostgreSQL escucha solo en `127.0.0.1:5433`; el usuario es `spotify`, la base `spotify_history` y la contraseña generada queda en `.env`. El archivo `docker-compose.override.yml` conecta Astro a la misma red de PostgreSQL incluso después de `astro dev restart`.

La API **Recently Played** entrega hasta 50 eventos por petición y no reconstruye un año completo de escucha anterior al inicio de la captura. El dashboard indica las fechas realmente registradas; los recuentos corresponden a eventos capturados, no a minutos reales escuchados. Para un Wrapped retrospectivo haría falta el archivo **Streaming History** exportado por Spotify. La captura futura continúa cada 30 minutos mientras Airflow esté ejecutándose; si se detiene más tiempo del que cubren los eventos recientes, puede haber huecos.

Para abrir el dashboard de nuevo: `http://127.0.0.1:8501`. Para parar los contenedores del dashboard y la base sin borrar los datos: `docker compose -f compose.spotify.yml down`. El volumen `spotify_history_data` conserva el historial.

## DAGs de ejemplo incluidos

| DAG | Archivo | Qué enseña |
|-----|---------|------------|
| **Astronauts ETL** | [`example_astronauts.py`](dags/example_astronauts.py) | DAG original de Astronomer. TaskFlow API, dynamic task mapping y llamadas a APIs. |
| **Weather ETL** | [`example_weather_etl.py`](dags/example_weather_etl.py) | Pipeline ETL completo (extract → transform → chart → load). Genera un gráfico de temperatura con matplotlib y lo muestra en la UI de Airflow. API gratuita Open-Meteo, sin credenciales. |
| **Spotify XCom** | [`example_spotify_xcom.py`](dags/example_spotify_xcom.py) | Concepto de XCom explicado paso a paso. Pipeline lineal: obtener canciones → rankear top 5 → mostrar resumen. |
| **Spotify Wrapped** | [`spotify_wrapped.py`](dags/spotify_wrapped.py) | Reproducciones recientes → XCom → PostgreSQL con `SQLExecuteQueryOperator` → dashboard visual. |
| **Spotify Trends** | [`example_spotify_trends.py`](dags/example_spotify_trends.py) | Branching, tareas en paralelo y trigger rules. Simula un análisis de tendencias musicales con caminos condicionales (pop vs rock). |
| **S3 Pipeline** | [`example_s3_pipeline.py`](dags/example_s3_pipeline.py) | Integración con Amazon S3 (free tier). Genera un CSV de ventas, lo sube a S3 y lo lee de vuelta. Requiere configurar conexión AWS. |
| **Task States** | [`example_task_states.py`](dags/example_task_states.py) | Muestra los estados más comunes de las tareas: success, failed, up_for_retry, upstream_failed y skipped. Ideal para entender los colores de la UI. |
| **Ejercicio: Movie Pipeline** | [`exercise_movie_pipeline.py`](dags/exercise_movie_pipeline.py) | Esqueleto incompleto con TODOs para que los alumnos practiquen. Usa la API de OMDb (películas). |

---

## Autorizar Spotify para el DAG de XCom

La app de Spotify debe tener registrada la Redirect URI `http://127.0.0.1:8888/callback`. Para el DAG de XCom, escribe `SPOTIFY_CLIENT_ID` y `SPOTIFY_CLIENT_SECRET` directamente en [`example_spotify_xcom.py`](dags/example_spotify_xcom.py) y ejecuta desde la raíz del proyecto:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\spotify_authorize.ps1
```

En macOS o Linux ejecuta `python3 scripts/spotify_authorize.py`.

El asistente abre Spotify en el navegador, recibe los permisos `playlist-read-private` y `user-read-recently-played`, comprueba que la cuenta puede leer la playlist y guarda `SPOTIFY_REFRESH_TOKEN` en `.env` y en el DAG sin mostrarlo. Si el navegador no se abre, copia la URL que imprime el script en la terminal. Después, vuelve a ejecutar el DAG en Airflow. El ranking cuenta cuántas veces aparece cada canción de la lista entre hasta 50 reproducciones recientes de la cuenta; Spotify no ofrece el total histórico por canción en la API. Estas reproducciones cuentan aunque se hayan escuchado fuera de la playlist. Si compartes el código, sustituye las credenciales por los valores `TU_...`.

---

# Contenido del proyecto (Astronomer)

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
  - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://docs.astronomer.io/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Levantar el entorno local de Airflow

## Requisitos previos

- **Docker Desktop** instalado y corriendo (icono de la ballena activo).
- **Astro CLI** instalado. Verifica con:
  ```bash
  astro version
  ```

## Paso 1: Arrancar Airflow

Desde la raíz del proyecto, ejecuta:

Si aún no has ejecutado ninguno de los scripts de Spotify Wrapped, crea primero la red compartida una sola vez con `docker network create spotify_history_net`. Si ya existe, continúa con el arranque.

```bash
astro dev start
```

Esto levanta 4 contenedores Docker:

| Contenedor | Función |
|------------|---------|
| **Postgres** | Base de datos de metadatos de Airflow |
| **Webserver** | Sirve la interfaz web (UI) |
| **Scheduler** | Programa y lanza las tareas |
| **Triggerer** | Gestiona tareas diferidas (deferrable) |

Verifica que los 4 están corriendo:

```bash
docker ps
```

> Si el puerto 8080 o 5432 ya están ocupados, para los contenedores que los usen o cambia los puertos en el archivo de configuración.

## Paso 2: Acceder a la UI de Airflow

Abre en el navegador:

```
http://localhost:8080
```

Credenciales por defecto:
- **Usuario**: `admin`
- **Contraseña**: `admin`

## Paso 3: Probar el DAG de Astronautas

1. En la UI, busca el DAG **`example_astronauts`** en la lista.
2. Si aparece como pausado (toggle gris), haz click en el toggle para activarlo (se pondrá azul). Este DAG viene con `is_paused_upon_creation=False`, así que debería estar activo por defecto.
3. Haz click en el botón **"Trigger DAG"** (▶) para lanzar una ejecución manual.
4. Entra al DAG y observa:
   - **Vista Graph**: verás `get_astronauts` conectado a múltiples instancias de `print_astronaut_craft` (una por cada astronauta en el espacio). Esto es **dynamic task mapping**.
   - **Vista Grid**: verás el estado de cada tarea (verde = success).
5. Haz click en cualquier instancia de `print_astronaut_craft` → pestaña **Log** para ver el nombre del astronauta y su nave.

## Paso 4: Parar el entorno

Cuando termines, para los contenedores con:

```bash
astro dev stop
```

Para eliminar los contenedores y volúmenes (reset completo):

```bash
astro dev kill
```

# Deploy Your Project to Astronomer

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

# Contact

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
