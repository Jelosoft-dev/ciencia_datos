"""Verifica los datos cargados por pipeline.py consultando data.duckdb con SQL.

Uso, desde la raíz del repositorio:
    python u2-pipeline/entregas/Jelosoft-dev/verificar_datos.py
"""

import sys
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent / "data.duckdb"
ESQUEMA = "rick_and_morty"


def verificar(db_path: Path = DB_PATH) -> dict:
    """Ejecuta las consultas de control y devuelve sus resultados en un diccionario."""
    if not db_path.exists():
        raise FileNotFoundError(f"No existe {db_path}. Ejecuta primero pipeline.py.")

    with duckdb.connect(str(db_path), read_only=True) as con:
        tablas = [
            fila[0]
            for fila in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = ? ORDER BY table_name",
                [ESQUEMA],
            ).fetchall()
        ]
        total = con.execute(f"SELECT COUNT(*) FROM {ESQUEMA}.personajes").fetchone()[0]
        ids_duplicados = con.execute(
            f"SELECT COUNT(*) - COUNT(DISTINCT id) FROM {ESQUEMA}.personajes"
        ).fetchone()[0]
        por_estado = con.execute(
            f"SELECT estado, COUNT(*) AS n FROM {ESQUEMA}.personajes "
            "GROUP BY estado ORDER BY n DESC"
        ).fetchall()
        top_especies = con.execute(
            f"SELECT especie, COUNT(*) AS n FROM {ESQUEMA}.personajes "
            "GROUP BY especie ORDER BY n DESC LIMIT 5"
        ).fetchall()
        mas_episodios = con.execute(
            f"SELECT nombre, cantidad_episodios FROM {ESQUEMA}.personajes "
            "WHERE estado = 'Alive' ORDER BY cantidad_episodios DESC, id LIMIT 5"
        ).fetchall()

    return {
        "tablas": tablas,
        "total": total,
        "ids_duplicados": ids_duplicados,
        "por_estado": por_estado,
        "top_especies": top_especies,
        "mas_episodios": mas_episodios,
    }


def imprimir(resultado: dict) -> None:
    print(f"Tablas en el esquema '{ESQUEMA}': {', '.join(resultado['tablas'])}")
    print(f"Total de personajes: {resultado['total']}")
    print(f"IDs duplicados: {resultado['ids_duplicados']}")
    print("\nPersonajes por estado:")
    for estado, n in resultado["por_estado"]:
        print(f"  {estado:<10} {n:>5}")
    print("\nTop 5 especies:")
    for especie, n in resultado["top_especies"]:
        print(f"  {especie:<20} {n:>5}")
    print("\nPersonajes vivos con más episodios:")
    for nombre, n in resultado["mas_episodios"]:
        print(f"  {nombre:<25} {n:>3} episodios")


def main() -> int:
    try:
        resultado = verificar()
    except FileNotFoundError as error:
        print(error, file=sys.stderr)
        return 1
    imprimir(resultado)
    return 0


if __name__ == "__main__":
    sys.exit(main())
