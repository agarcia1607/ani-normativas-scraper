# ANI Normativas Scraper — Airflow

Scraping de normativas de la [ANI](https://www.ani.gov.co) orquestado en Airflow con tres etapas: **Extracción → Validación → Escritura**.

---

## Estructura

```
src/
  extraction/extractor.py   # scraping ANI (lógica original intacta)
  validation/validator.py   # validación por tipo/regex/obligatoriedad
  writing/
    db.py                   # conexión Postgres vía env vars
    writer.py               # deduplicación e inserción
configs/
  validation_rules.yaml     # reglas configurables sin tocar código
dags/
  ani_scraping_dag.py       # DAG: extract → validate → write
schema/
  ddl.sql                   # CREATE TABLE IF NOT EXISTS
```

---

## Levantar el entorno

**1. Variables de entorno**

```bash
cp .env.example .env
# .env ya tiene los defaults correctos para el docker-compose local
```

**2. Crear las tablas** (una sola vez)

```bash
docker compose up postgres -d
docker compose run --rm scheduler bash -c \
  "psql postgresql://airflow:airflow@postgres/airflow -f /opt/airflow/schema/ddl.sql"
```

**3. Inicializar Airflow y levantar**

```bash
docker compose run --rm scheduler airflow db init
docker compose up -d
```

La UI queda disponible en `http://localhost:8080` (usuario: `airflow`, contraseña: `airflow`).

---

## Ejecutar el DAG

Desde la UI: activar el DAG `ani_normativas_scraping` y hacer clic en **Trigger DAG**.

O por CLI:

```bash
docker compose exec scheduler airflow dags trigger ani_normativas_scraping
```

El DAG también corre automáticamente cada día a las 06:00 UTC.

---

## Reglas de validación

Las reglas están en `configs/validation_rules.yaml`. Cada campo puede configurar:

| Clave      | Descripción                                              |
|------------|----------------------------------------------------------|
| `type`     | `str`, `int`, `bool`, `date`                             |
| `regex`    | Patrón que debe cumplir el valor                         |
| `required` | `true` → descarta la fila; `false` → deja el campo NULL  |

YAML es una opción común en pipelines de datos y configuración operativa. No es necesario modificar código para cambiar las reglas.

---

## Logs relevantes

Cada run emite en el task `write`:

```
[RESUMEN] Extraídos: N | Descartados por validación: N | Insertados: N
```

Los logs detallados de deduplicación quedan en el task `write` y los de validación en `validate`.
