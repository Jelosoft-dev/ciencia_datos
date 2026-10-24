# Informe U2: Mi primer pipeline de datos con dlt y DuckDB

**Nombre:** Jesús López

**Fecha:** 24/10/2026

**Rama:** `u2/Jelosoft-dev`

## Qué se hizo

Construí un pipeline que extrae todos los personajes de la API pública de Rick and Morty
(`https://rickandmortyapi.com/api/character`), los limpia en un recurso de dlt y los carga en
`data.duckdb`, en el esquema `rick_and_morty` y la tabla `personajes`. El orden fue: primero
la función que recorre la paginación, después el recurso y la fuente de dlt, luego el script
`verificar_datos.py` con las consultas SQL y, al final, las pruebas con pytest.

Elegí esta API porque no requiere clave y trae campos anidados y listas, lo que permite ver
cómo dlt infiere y normaliza el esquema.

## Decisiones de diseño

**Paginación explícita con `requests`.** La API indica la página siguiente en `info.next`.
Escribí `obtener_paginas` como un generador que sigue ese enlace hasta que vale `None`. El
recurso entrega una página por vez. La sesión HTTP se recibe como parámetro, lo que me permitió
simular la API en las pruebas.

**`write_disposition="replace"` con `primary_key="id"`.** Cada ejecución descarga el conjunto
completo, que es pequeño, así que reemplazar la tabla es lo más simple y garantiza que no haya
duplicados si corro el pipeline dos veces. Con `append` cada ejecución duplicaría las filas; con
`merge` tendría deduplicación, pero agrega complejidad que este volumen no justifica.

**Limpieza antes de cargar.** `limpiar_personaje` renombra los campos al español, aplana
`origin` y `location` a su nombre, convierte la cadena vacía de `type` en nulo, descarta el
campo `url` (redundante con `id`) y transforma las URLs de episodios en números enteros.

**Errores visibles.** Un 404 significa “no hay resultados” y lo trato como respuesta vacía. Si
un error persiste tras los reintentos, se propaga: prefiero que el pipeline falle a que cargue
datos incompletos sin avisar.

## Cómo lidié con el esquema automático de dlt

dlt infirió solo los tipos: `id` como `BIGINT`, textos como `VARCHAR` y `creado` como
`TIMESTAMP WITH TIME ZONE` a partir de la cadena ISO. La lista `episodios` no se guardó como
columna: dlt la normalizó en una tabla hija, `personajes__episodios`, unida a la tabla padre
por las columnas `_dlt_id` y `_dlt_parent_id`. Además agregó sus tablas de control
(`_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`).

El problema que apareció: al probar con datos simulados donde `tipo` era siempre nulo, dlt avisó
que no podía inferir el tipo de esa columna y **no la creó**. Es decir, el esquema de la tabla
dependía de los datos de cada carga. Lo resolví con pistas de tipo en el decorador
(`columns={"tipo": {"data_type": "text"}, ...}`) y agregué una prueba que verifica que la
columna existe aunque llegue vacía.

## Evidencias de ejecución
```shell
python u2-pipeline/entregas/Jelosoft-dev/pipeline.py

La API respondió 429; reintento en 10.0 s (https://rickandmortyapi.com/api/character?page=31)
Pipeline rick_and_morty_pipeline load step finished in 0.22 seconds
1 load package(s) were loaded to destination duckdb and into dataset rick_and_morty
The duckdb destination used duckdb:////workspaces/ciencia_datos/u2-pipeline/entregas/Jelosoft-dev/data.duckdb location to store data
Load package 1792861590.6600194 is LOADED and contains no failed jobs
Base de datos generada en: /workspaces/ciencia_datos/u2-pipeline/entregas/Jelosoft-dev/data.duckdb
```

```shell
uv run python $D/verificar_datos.py

Alive        439
  Dead         287
  unknown      100

Top 5 especies:
  Human                  366
  Alien                  205
  Humanoid                68
  Animal                  55
  Robot                   51

Personajes vivos con más episodios:
  Rick Sanchez               51 episodios
  Morty Smith                51 episodios
  Summer Smith               42 episodios
  Beth Smith                 42 episodios
  Jerry Smith                39 episodios
```

```shell
uv run pytest $D -v

================================= test session starts =================================
platform linux -- Python 3.12.11, pytest-9.1.1, pluggy-1.6.0 -- /workspaces/ciencia_datos/.venv/bin/python
cachedir: .pytest_cache
rootdir: /workspaces/ciencia_datos
configfile: pyproject.toml
plugins: anyio-4.15.1, platformdirs-4.12.3
collected 12 items                                                                                                                                                             

u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_recurso_recorre_paginas_y_tiene_estructura_limpia PASSED                                                        [  8%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_recurso_declara_clave_primaria_y_reemplazo PASSED                                                               [ 16%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_respuesta_vacia_no_produce_registros[results-vacio] PASSED                                                      [ 25%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_respuesta_vacia_no_produce_registros[cuerpo-vacio] PASSED                                                       [ 33%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_respuesta_vacia_no_produce_registros[404-sin-resultados] PASSED                                                 [ 41%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_error_de_conexion_se_propaga PASSED                                                                             [ 50%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_error_del_servidor_se_propaga PASSED                                                                            [ 58%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_429_se_reintenta_respetando_retry_after PASSED                                                                  [ 66%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_429_persistente_falla_tras_agotar_intentos PASSED                                                               [ 75%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_pausa_entre_paginas PASSED                                                                                      [ 83%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_pipeline_carga_en_duckdb_y_verificacion PASSED                                                                  [ 91%]
u2-pipeline/entregas/Jelosoft-dev/test_pipeline.py::test_verificar_sin_base_de_datos PASSED                                                                              [100%]

================================= 12 passed in 1.51s =================================
```

## Qué me pareció trabajar con DuckDB

DuckDB resultó muy cómodo: es un único archivo, no necesita servidor y se consulta desde Python con SQL estándar en pocas líneas. Abrí la base en modo `read_only` en el script de verificación para no bloquearla ni modificarla por accidente. Como contra, un archivo DuckDB admite un solo proceso escritor a la vez, por lo que no conviene tenerlo abierto en otra herramienta mientras corre el pipeline.

