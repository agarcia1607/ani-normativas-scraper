"""
writer.py
---------
Escritura en PostgreSQL con deduplicación.

Conserva íntegramente la lógica de `insert_new_records` e
`insert_regulations_component` de la Lambda original.

Tablas destino (usando el Postgres del docker-compose de Airflow):
  - regulations
  - regulations_component

Punto de entrada público: run_writing(records: list[dict]) -> dict
"""

import logging

import pandas as pd

from src.writing.db import DatabaseManager

logger = logging.getLogger(__name__)

REGULATIONS_TABLE = "regulations"
REGULATIONS_COMPONENT_TABLE = "regulations_component"
ENTITY_VALUE = "Agencia Nacional de Infraestructura"


# ---------------------------------------------------------------------------
# Helpers de inserción (lógica idéntica a la Lambda)
# ---------------------------------------------------------------------------

def _insert_regulations_component(db: DatabaseManager, new_ids: list) -> int:
    if not new_ids:
        logger.info("Sin nuevos IDs para regulations_component.")
        return 0
    df = pd.DataFrame(new_ids, columns=["regulations_id"])
    df["components_id"] = 7
    inserted = db.bulk_insert(df, REGULATIONS_COMPONENT_TABLE)
    logger.info("Componentes insertados: %d", inserted)
    return inserted


def _insert_new_records(db: DatabaseManager, df: pd.DataFrame, entity: str) -> tuple[int, str]:
    """
    Lógica de deduplicación e inserción copiada de la Lambda.
    Nombres de tabla corregidos a `regulations` / `regulations_component`.
    """
    # 1. Registros existentes en BD
    query = """
        SELECT title, created_at, entity, COALESCE(external_link, '') AS external_link
        FROM {}
        WHERE entity = %s
    """.format(REGULATIONS_TABLE)

    existing = db.execute_query(query, (entity,))
    db_df = (
        pd.DataFrame(existing, columns=["title", "created_at", "entity", "external_link"])
        if existing
        else pd.DataFrame(columns=["title", "created_at", "entity", "external_link"])
    )
    logger.info("Registros existentes en BD para '%s': %d", entity, len(db_df))

    # 2. Filtrar por entidad
    entity_df = df[df["entity"] == entity].copy()
    if entity_df.empty:
        return 0, f"Sin registros para entidad {entity}"
    logger.info("Registros a procesar para '%s': %d", entity, len(entity_df))

    # 3. Normalizar para comparación
    for frame in [db_df, entity_df]:
        if not frame.empty:
            frame["created_at"] = frame["created_at"].astype(str)
            frame["external_link"] = frame["external_link"].fillna("").astype(str)
            frame["title"] = frame["title"].astype(str).str.strip()

    # 4. Detectar duplicados contra BD
    if db_df.empty:
        new_records = entity_df.copy()
        duplicates_found = 0
        logger.info("Sin registros previos — todos son nuevos.")
    else:
        entity_df["unique_key"] = (
            entity_df["title"] + "|" + entity_df["created_at"] + "|" + entity_df["external_link"]
        )
        db_df["unique_key"] = (
            db_df["title"] + "|" + db_df["created_at"] + "|" + db_df["external_link"]
        )
        existing_keys = set(db_df["unique_key"])
        entity_df["is_duplicate"] = entity_df["unique_key"].isin(existing_keys)
        new_records = entity_df[~entity_df["is_duplicate"]].copy()
        duplicates_found = len(entity_df) - len(new_records)
        logger.info("Duplicados contra BD: %d", duplicates_found)

    # 5. Remover duplicados internos
    before = len(new_records)
    new_records = new_records.drop_duplicates(
        subset=["title", "created_at", "external_link"], keep="first"
    )
    internal_dupes = before - len(new_records)
    if internal_dupes:
        logger.info("Duplicados internos removidos: %d", internal_dupes)

    total_dupes = duplicates_found + internal_dupes
    logger.info("Total duplicados descartados: %d", total_dupes)

    if new_records.empty:
        return 0, f"Sin registros nuevos para entidad {entity} tras deduplicación"

    # 6. Limpiar columnas auxiliares
    for col in ["unique_key", "is_duplicate"]:
        if col in new_records.columns:
            new_records = new_records.drop(columns=[col])

    logger.info("Registros a insertar: %d", len(new_records))

    # 7. Insertar
    try:
        inserted = db.bulk_insert(new_records, REGULATIONS_TABLE)
    except Exception as exc:
        msg = str(exc).lower()
        if "duplicate" in msg or "unique" in msg:
            logger.warning("Algunos registros ya existían (constraint único). %s", exc)
            return 0, f"Registros duplicados detectados por constraint para {entity}"
        raise

    logger.info("Registros insertados: %d", inserted)

    # 8. IDs recién insertados (últimos N por entidad)
    ids_result = db.execute_query(
        f"SELECT id FROM {REGULATIONS_TABLE} WHERE entity = %s ORDER BY id DESC LIMIT %s",
        (entity, inserted),
    )
    new_ids = [row[0] for row in ids_result]

    # 9. Componentes
    _insert_regulations_component(db, new_ids)

    stats = (
        f"Procesados: {len(entity_df)} | "
        f"Existentes en BD: {len(db_df)} | "
        f"Duplicados omitidos: {total_dupes} | "
        f"Insertados: {inserted}"
    )
    return inserted, stats


# ---------------------------------------------------------------------------
# Punto de entrada público
# ---------------------------------------------------------------------------

def run_writing(records: list[dict]) -> dict:
    """
    Persiste `records` en Postgres aplicando deduplicación.

    Args:
        records: lista de dicts ya validados.

    Returns:
        dict con claves: inserted, discarded_dupes, message.
    """
    if not records:
        logger.warning("run_writing recibió lista vacía — nada que escribir.")
        return {"inserted": 0, "discarded_dupes": 0, "message": "Sin registros para escribir"}

    df = pd.DataFrame(records)
    db = DatabaseManager()

    if not db.connect():
        raise RuntimeError("No se pudo conectar a la base de datos.")

    try:
        inserted, message = _insert_new_records(db, df, ENTITY_VALUE)
        logger.info("=== RESULTADO ESCRITURA === %s", message)
        return {
            "inserted": inserted,
            "message": message,
        }
    finally:
        db.close()
