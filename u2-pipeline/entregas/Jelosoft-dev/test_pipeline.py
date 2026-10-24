"""Pruebas del pipeline. No usan la red: la API se simula con una sesión falsa."""

import duckdb
import pipeline
import pytest
import requests
import verificar_datos
from dlt.extract.exceptions import ResourceExtractionError

BASE = "https://api.falsa/api"


def personaje(id_: int, estado: str = "Alive", especie: str = "Human") -> dict:
    return {
        "id": id_,
        "name": f"Personaje {id_}",
        "status": estado,
        "species": especie,
        "type": "",
        "gender": "Male",
        "origin": {"name": "Earth (C-137)", "url": ""},
        "location": {"name": "Citadel of Ricks", "url": ""},
        "image": f"https://img/{id_}.jpeg",
        "episode": [f"{BASE}/episode/1", f"{BASE}/episode/{id_ + 1}"],
        "url": f"{BASE}/character/{id_}",
        "created": "2017-11-04T18:48:46.250Z",
    }


class RespuestaFalsa:
    def __init__(self, status_code: int = 200, cuerpo: dict | None = None):
        self.status_code = status_code
        self._cuerpo = cuerpo

    def json(self):
        return self._cuerpo

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"Error HTTP {self.status_code}")


class SesionFalsa:
    """Devuelve respuestas preparadas según la URL y registra las llamadas."""

    def __init__(self, respuestas: dict):
        self.respuestas = respuestas
        self.llamadas: list[str] = []

    def get(self, url, timeout=None):
        self.llamadas.append(url)
        respuesta = self.respuestas[url]
        if isinstance(respuesta, Exception):
            raise respuesta
        return respuesta


def sesion_dos_paginas() -> SesionFalsa:
    pagina2 = f"{BASE}/character?page=2"
    return SesionFalsa(
        {
            f"{BASE}/character": RespuestaFalsa(
                cuerpo={"info": {"next": pagina2}, "results": [personaje(1), personaje(2)]}
            ),
            pagina2: RespuestaFalsa(
                cuerpo={"info": {"next": None}, "results": [personaje(3, "Dead", "Alien")]}
            ),
        }
    )


def extraer(sesion) -> list[dict]:
    """Ejecuta el recurso dlt sin destino y devuelve los registros que produce."""
    return list(pipeline.personajes(base_url=BASE, session=sesion))


def test_recurso_recorre_paginas_y_tiene_estructura_limpia():
    sesion = sesion_dos_paginas()

    registros = extraer(sesion)

    assert len(sesion.llamadas) == 2
    assert [r["id"] for r in registros] == [1, 2, 3]
    assert set(registros[0]) == {
        "id",
        "nombre",
        "estado",
        "especie",
        "tipo",
        "genero",
        "origen",
        "ubicacion",
        "cantidad_episodios",
        "episodios",
        "imagen",
        "creado",
    }
    assert registros[0]["origen"] == "Earth (C-137)"
    assert registros[0]["tipo"] is None
    assert registros[0]["episodios"] == [1, 2]
    assert registros[0]["cantidad_episodios"] == 2


def test_recurso_declara_clave_primaria_y_reemplazo():
    esquema = pipeline.personajes().compute_table_schema()
    assert esquema["name"] == "personajes"
    assert esquema["write_disposition"] == "replace"
    assert esquema["columns"]["id"]["primary_key"] is True


@pytest.mark.parametrize(
    "respuesta",
    [
        RespuestaFalsa(cuerpo={"info": {"next": None}, "results": []}),
        RespuestaFalsa(cuerpo={}),
        RespuestaFalsa(status_code=404, cuerpo={"error": "There is nothing here"}),
    ],
    ids=["results-vacio", "cuerpo-vacio", "404-sin-resultados"],
)
def test_respuesta_vacia_no_produce_registros(respuesta):
    sesion = SesionFalsa({f"{BASE}/character": respuesta})
    assert extraer(sesion) == []


def test_error_de_conexion_se_propaga():
    sesion = SesionFalsa({f"{BASE}/character": requests.ConnectionError("sin red")})
    # dlt envuelve la excepción original; comprobamos que la causa es el error de red.
    with pytest.raises(ResourceExtractionError) as error:
        extraer(sesion)
    assert isinstance(error.value.__cause__, requests.ConnectionError)


def test_error_del_servidor_se_propaga():
    sesion = SesionFalsa({f"{BASE}/character": RespuestaFalsa(status_code=500)})
    with pytest.raises(requests.HTTPError):
        list(pipeline.obtener_paginas(f"{BASE}/character", session=sesion))


def test_pipeline_carga_en_duckdb_y_verificacion(tmp_path):
    db = tmp_path / "data.duckdb"

    pipeline.ejecutar(db, tmp_path / ".dlt", base_url=BASE, session=sesion_dos_paginas())

    with duckdb.connect(str(db), read_only=True) as con:
        total = con.execute("SELECT COUNT(*) FROM rick_and_morty.personajes").fetchone()[0]
        hijos = con.execute("SELECT COUNT(*) FROM rick_and_morty.personajes__episodios").fetchone()[
            0
        ]
        columnas = {
            fila[0]
            for fila in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'rick_and_morty' AND table_name = 'personajes'"
            ).fetchall()
        }
    assert total == 3
    assert hijos == 6
    # "tipo" llega siempre vacío en estos datos; la pista de tipo garantiza que exista.
    assert {"id", "nombre", "tipo", "creado"} <= columnas
    assert "episodios" not in columnas  # la lista va a la tabla hija

    resultado = verificar_datos.verificar(db)
    assert resultado["total"] == 3
    assert resultado["ids_duplicados"] == 0
    assert dict(resultado["por_estado"]) == {"Alive": 2, "Dead": 1}


def test_verificar_sin_base_de_datos(tmp_path):
    with pytest.raises(FileNotFoundError):
        verificar_datos.verificar(tmp_path / "no_existe.duckdb")
