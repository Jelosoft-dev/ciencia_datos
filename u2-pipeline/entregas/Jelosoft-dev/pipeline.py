"""Pipeline de datos: Rick and Morty API -> dlt -> DuckDB.

Extrae todos los personajes de https://rickandmortyapi.com (API pública, sin clave),
los limpia en un recurso de dlt y los carga en ``data.duckdb``, dentro del esquema
``rick_and_morty`` y la tabla ``personajes``.

Uso, desde la raíz del repositorio:
    python u2-pipeline/entregas/Jelosoft-dev/pipeline.py
"""

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


def obtener_paginas(url: str, session: Any = None) -> Iterator[list[dict]]:
    """Recorre la paginación de la API y devuelve la lista ``results`` de cada página.

    La API indica la siguiente página en ``info.next`` (``None`` en la última).
    Un 404 significa "no hay resultados" y se trata como respuesta vacía.
    Los errores de conexión o de HTTP (5xx, etc.) se propagan para que el
    pipeline falle de forma visible en lugar de cargar datos incompletos.
    """
    http = session or requests.Session()
    siguiente: str | None = url
    while siguiente:
        respuesta = http.get(siguiente, timeout=TIMEOUT_SEGUNDOS)
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
