"""
db.py
-----
Gestión de conexión a PostgreSQL.

Reemplaza el DatabaseManager original eliminando la dependencia de
AWS Secrets Manager. Las credenciales se leen de variables de entorno:

  DB_HOST      (default: postgres)
  DB_PORT      (default: 5432)
  DB_NAME      (default: airflow)
  DB_USER      (default: airflow)
  DB_PASSWORD  (default: airflow)

La lógica de bulk_insert y execute_query es idéntica a la Lambda.
"""

import logging
import os

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)


def _get_conn_params() -> dict:
    return {
        "host": os.environ.get("DB_HOST", "postgres"),
        "port": int(os.environ.get("DB_PORT", 5432)),
        "dbname": os.environ.get("DB_NAME", "airflow"),
        "user": os.environ.get("DB_USER", "airflow"),
        "password": os.environ.get("DB_PASSWORD", "airflow"),
    }


class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        try:
            params = _get_conn_params()
            self.connection = psycopg2.connect(**params)
            self.cursor = self.connection.cursor()
            logger.info(
                "Conexión establecida — host=%s db=%s", params["host"], params["dbname"]
            )
            return True
        except Exception as exc:
            logger.error("Error de conexión a la base de datos: %s", exc)
            return False

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.debug("Conexión cerrada.")

    def execute_query(self, query: str, params=None) -> list:
        if not self.cursor:
            raise RuntimeError("Base de datos no conectada.")
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def bulk_insert(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Inserción masiva de un DataFrame en `table_name`.
        Idéntica a la Lambda original.
        """
        if not self.connection or not self.cursor:
            raise RuntimeError("Base de datos no conectada.")

        try:
            df = df.astype(object).where(pd.notnull(df), None)
            columns_sql = ", ".join(f'"{col}"' for col in df.columns)
            placeholders = ", ".join(["%s"] * len(df.columns))
            query = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
            records = [tuple(x) for x in df.values]
            self.cursor.executemany(query, records)
            self.connection.commit()
            return len(df)
        except Exception as exc:
            self.connection.rollback()
            raise RuntimeError(f"Error insertando en {table_name}: {exc}") from exc
