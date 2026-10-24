"""Pipeline de datos: Rick and Morty API -> dlt -> DuckDB.

Extrae todos los personajes de https://rickandmortyapi.com (API pública, sin clave),
los limpia en un recurso de dlt y los carga en ``data.duckdb``, dentro del esquema
``rick_and_morty`` y la tabla ``personajes``.

Uso, desde la raíz del repositorio:
    python u2-pipeline/entregas/Jelosoft-dev/pipeline.py
"""

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dlt
import requests

BASE_URL = "https://rickandmortyapi.com/api"
CARPETA = Path(__file__).resolve().parent
DB_PATH = CARPETA / "data.duckdb"
NOMBRE_PIPELINE = "rick_and_morty_pipeline"
ESQUEMA = "rick_and_morty"
TIMEOUT_SEGUNDOS = 15

# La API limita las peticiones seguidas y responde 429 (Too Many Requests).
PAUSA_ENTRE_PAGINAS = 0.3  # segundos entre una página y la siguiente
MAX_INTENTOS = 5  # intentos por página ante 429 o errores 5xx
ESPERA_BASE = 1.0  # segundos; se duplica en cada reintento (1, 2, 4, 8...)
CODIGOS_REINTENTABLES = {429, 500, 502, 503, 504}

# Se guarda en una variable para que las pruebas puedan reemplazarla y no esperar.
esperar = time.sleep


def segundos_de_espera(respuesta: Any, intento: int) -> float:
    """Usa la cabecera ``Retry-After`` si la API la envía; si no, espera exponencial."""
    cabeceras = getattr(respuesta, "headers", None) or {}
    valor = cabeceras.get("Retry-After")
    if valor is not None:
        try:
            return max(float(valor), 0.0)
        except ValueError:
            pass
    return ESPERA_BASE * 2**intento


def pedir_con_reintentos(http: Any, url: str) -> Any:
    """Hace el GET y lo repite, esperando cada vez más, si la API responde 429 o 5xx.

    Si tras ``MAX_INTENTOS`` sigue fallando, devuelve la última respuesta para que
    ``raise_for_status`` la convierta en error.
    """
    for intento in range(MAX_INTENTOS):
        respuesta = http.get(url, timeout=TIMEOUT_SEGUNDOS)
        if respuesta.status_code not in CODIGOS_REINTENTABLES or intento == MAX_INTENTOS - 1:
            return respuesta
        segundos = segundos_de_espera(respuesta, intento)
        print(f"La API respondió {respuesta.status_code}; reintento en {segundos:.1f} s ({url})")
        esperar(segundos)
    return respuesta


def obtener_paginas(url: str, session: Any = None) -> Iterator[list[dict]]:
    """Recorre la paginación de la API y devuelve la lista ``results`` de cada página.

    La API indica la siguiente página en ``info.next`` (``None`` en la última).
    Un 404 significa "no hay resultados" y se trata como respuesta vacía.
    Ante 429 o 5xx se reintenta con espera; si el error persiste, o si falla la
    conexión, la excepción se propaga para que el pipeline falle de forma visible
    en lugar de cargar datos incompletos.
    """
    http = session or requests.Session()
    siguiente: str | None = url
    primera = True
    while siguiente:
        if not primera:
            esperar(PAUSA_ENTRE_PAGINAS)
        primera = False
        respuesta = pedir_con_reintentos(http, siguiente)
        if respuesta.status_code == 404:
            return
        respuesta.raise_for_status()
        cuerpo = respuesta.json() or {}
        resultados = cuerpo.get("results") or []
        if not resultados:
            return
        yield resultados
        siguiente = (cuerpo.get("info") or {}).get("next")


def limpiar_personaje(crudo: dict) -> dict:
    """Convierte un personaje de la API en un registro plano y con nombres claros."""
    episodios = [int(url.rstrip("/").rsplit("/", 1)[-1]) for url in crudo.get("episode", [])]
    return {
        "id": crudo["id"],
        "nombre": crudo.get("name"),
        "estado": crudo.get("status"),
        "especie": crudo.get("species"),
        "tipo": crudo.get("type") or None,
        "genero": crudo.get("gender"),
        "origen": (crudo.get("origin") or {}).get("name"),
        "ubicacion": (crudo.get("location") or {}).get("name"),
        "cantidad_episodios": len(episodios),
        # Lista anidada: dlt la normaliza en la tabla hija personajes__episodios.
        "episodios": episodios,
        "imagen": crudo.get("image"),
        "creado": crudo.get("created"),
    }


# Pistas de tipo: si en una carga "tipo" llega siempre vacío, dlt no puede inferir su
# tipo y no crea la columna; así el esquema de la tabla es estable entre ejecuciones.
COLUMNAS = {
    "tipo": {"data_type": "text", "nullable": True},
    "creado": {"data_type": "timestamp"},
}


@dlt.resource(
    name="personajes",
    primary_key="id",
    write_disposition="replace",
    columns=COLUMNAS,
)
def personajes(base_url: str = BASE_URL, session: Any = None) -> Iterator[list[dict]]:
    """Recurso dlt con todos los personajes, ya limpios. Entrega una página por vez."""
    for pagina in obtener_paginas(f"{base_url}/character", session=session):
        yield [limpiar_personaje(p) for p in pagina]


@dlt.source(name="rick_and_morty")
def rick_and_morty(base_url: str = BASE_URL, session: Any = None):
    """Fuente dlt que agrupa los recursos de la API."""
    return personajes(base_url=base_url, session=session)


def crear_pipeline(db_path: Path = DB_PATH, pipelines_dir: Path | None = None) -> dlt.Pipeline:
    """Crea el pipeline apuntando a un archivo DuckDB concreto."""
    return dlt.pipeline(
        pipeline_name=NOMBRE_PIPELINE,
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name=ESQUEMA,
        pipelines_dir=str(pipelines_dir) if pipelines_dir else None,
    )


def ejecutar(
    db_path: Path = DB_PATH,
    pipelines_dir: Path | None = None,
    base_url: str = BASE_URL,
    session: Any = None,
):
    """Ejecuta extracción, normalización y carga. Devuelve el ``LoadInfo`` de dlt."""
    pipeline = crear_pipeline(db_path, pipelines_dir)
    return pipeline.run(rick_and_morty(base_url=base_url, session=session))


def main() -> None:
    info = ejecutar()
    print(info)
    print(f"Base de datos generada en: {DB_PATH}")


if __name__ == "__main__":
    main()
