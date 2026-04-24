"""
dags/ani_scraping_dag.py
------------------------
DAG: ani_normativas_scraping
Secuencia: extract → validate → write

Variables de entorno esperadas (definidas en .env y docker-compose):
  NUM_PAGES_TO_SCRAPE   (default: 9)
  VALIDATION_RULES_PATH (default: /opt/airflow/configs/validation_rules.yaml)
  DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def task_extract(**context) -> None:
    """
    Scrapea las páginas de ANI y empuja list[dict] a XCom.
    """
    from src.extraction.extractor import run_extraction

    num_pages = int(os.environ.get("NUM_PAGES_TO_SCRAPE", 9))
    logger.info("[EXTRACT] Iniciando — páginas: %d", num_pages)

    records = run_extraction(num_pages=num_pages)

    logger.info("[EXTRACT] Total extraídos: %d", len(records))

    # XCom solo acepta tipos JSON-serializable → list[dict] ✓
    context["ti"].xcom_push(key="raw_records", value=records)
    context["ti"].xcom_push(key="extracted_count", value=len(records))


def task_validate(**context) -> None:
    """
    Valida los registros recibidos por XCom y empuja los aprobados.
    """
    from pathlib import Path
    from src.validation.validator import run_validation

    ti = context["ti"]
    raw_records: list[dict] = ti.xcom_pull(task_ids="extract", key="raw_records")

    if not raw_records:
        logger.warning("[VALIDATE] Sin registros para validar.")
        ti.xcom_push(key="validated_records", value=[])
        ti.xcom_push(key="discarded_count", value=0)
        return

    rules_path_str = os.environ.get(
        "VALIDATION_RULES_PATH",
        "/opt/airflow/configs/validation_rules.yaml",
    )
    logger.info("[VALIDATE] Reglas desde: %s", rules_path_str)
    logger.info("[VALIDATE] Registros a validar: %d", len(raw_records))

    validated = run_validation(raw_records, rules_path=Path(rules_path_str))

    discarded = len(raw_records) - len(validated)
    logger.info("[VALIDATE] Aprobados: %d | Descartados: %d", len(validated), discarded)

    ti.xcom_push(key="validated_records", value=validated)
    ti.xcom_push(key="discarded_count", value=discarded)


def task_write(**context) -> None:
    """
    Escribe los registros validados en Postgres con deduplicación.
    """
    from src.writing.writer import run_writing

    ti = context["ti"]
    validated_records: list[dict] = ti.xcom_pull(task_ids="validate", key="validated_records")
    extracted_count: int = ti.xcom_pull(task_ids="extract", key="extracted_count") or 0
    discarded_count: int = ti.xcom_pull(task_ids="validate", key="discarded_count") or 0

    if not validated_records:
        logger.warning("[WRITE] Sin registros validados — nada que escribir.")
        return

    logger.info("[WRITE] Registros a escribir: %d", len(validated_records))

    result = run_writing(validated_records)

    # -----------------------------------------------------------------------
    # Resumen final en log (requerido por la prueba)
    # -----------------------------------------------------------------------
    logger.info(
        "[RESUMEN] Extraídos: %d | Descartados por validación: %d | Insertados: %d | %s",
        extracted_count,
        discarded_count,
        result.get("inserted", 0),
        result.get("message", ""),
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ani_normativas_scraping",
    description="Scraping normativas ANI → Validación → Escritura en Postgres",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # diario a las 06:00 UTC
    catchup=False,
    tags=["ani", "scraping", "normativas"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    write = PythonOperator(
        task_id="write",
        python_callable=task_write,
    )

    extract >> validate >> write
