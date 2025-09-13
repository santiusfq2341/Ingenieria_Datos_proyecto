# dags/retenciones_control_y_carga.py
from __future__ import annotations
from datetime import datetime
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

MYSQL_CONN_ID = "mysql_docker"
DB_NAME = "retenciones_mensuales"
BATCH_SIZE = 5000

SQL_SELECT = f"""
SELECT
  no_secuencial_retencion,
  id_cliente,
  fecha_emision,
  cod_retencion,
  base_imponible,
  monto_retencion
FROM {DB_NAME}.retenciones
"""

# Tablas destino:
# - retenciones_correcto (SIN columna diferencias)
# - retenciones_errores  (CON columna diferencias)
DDL_CORRECTO = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.retenciones_correcto (
  no_secuencial_retencion CHAR(6) PRIMARY KEY,
  id_cliente VARCHAR(21) NOT NULL,
  fecha_emision DATE NOT NULL,
  cod_retencion CHAR(5) NOT NULL,
  base_imponible FLOAT(9,2) NOT NULL,
  monto_retencion FLOAT(9,2) NOT NULL,
  porcentaje_ret FLOAT(5,4)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

DDL_ERRORES = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.retenciones_errores (
  no_secuencial_retencion CHAR(6) PRIMARY KEY,
  id_cliente VARCHAR(21) NOT NULL,
  fecha_emision DATE NOT NULL,
  cod_retencion CHAR(5) NOT NULL,
  base_imponible FLOAT(9,2) NOT NULL,
  monto_retencion FLOAT(9,2) NOT NULL,
  porcentaje_ret FLOAT(5,4),
  diferencias FLOAT(9,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

UPSERT_CORRECTO = f"""
INSERT INTO {DB_NAME}.retenciones_correcto (
  no_secuencial_retencion, id_cliente, fecha_emision, cod_retencion,
  base_imponible, monto_retencion, porcentaje_ret
) VALUES (%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  id_cliente=VALUES(id_cliente),
  fecha_emision=VALUES(fecha_emision),
  cod_retencion=VALUES(cod_retencion),
  base_imponible=VALUES(base_imponible),
  monto_retencion=VALUES(monto_retencion),
  porcentaje_ret=VALUES(porcentaje_ret);
"""

UPSERT_ERRORES = f"""
INSERT INTO {DB_NAME}.retenciones_errores (
  no_secuencial_retencion, id_cliente, fecha_emision, cod_retencion,
  base_imponible, monto_retencion, porcentaje_ret, diferencias
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  id_cliente=VALUES(id_cliente),
  fecha_emision=VALUES(fecha_emision),
  cod_retencion=VALUES(cod_retencion),
  base_imponible=VALUES(base_imponible),
  monto_retencion=VALUES(monto_retencion),
  porcentaje_ret=VALUES(porcentaje_ret),
  diferencias=VALUES(diferencias);
"""


def _build_dataframes() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lee desde MySQL, calcula porcentaje_ret y diferencias, separa sin/con diferencias."""
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, schema=DB_NAME)
    df = hook.get_pandas_df(SQL_SELECT)

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Normalizaciones
    df["base_imponible"] = pd.to_numeric(df["base_imponible"], errors="coerce")
    df["monto_retencion"] = pd.to_numeric(
        df["monto_retencion"], errors="coerce")
    df["cod_retencion"] = df["cod_retencion"].astype(str).str.strip()
    df["fecha_emision"] = pd.to_datetime(
        df["fecha_emision"], errors="coerce").dt.strftime("%Y-%m-%d")

    # porcentaje_ret: 323A -> 0.02 ; 324B -> 0.01 ; otros -> NaN
    df["porcentaje_ret"] = np.where(df["cod_retencion"] == "323A", 0.02,
                                    np.where(df["cod_retencion"] == "324B", 0.01, np.nan))

    # diferencias = (base_imponible * porcentaje_ret) - monto_retencion  (según tu especificación)
    esperado = df["base_imponible"] * df["porcentaje_ret"]
    df["diferencias"] = (esperado - df["monto_retencion"]).round(2)

    # Split: NaN se consideran "con diferencias"
    sin_df = df[df["diferencias"].fillna(0).eq(0.00)].copy()
    con_df = df[df["diferencias"].fillna(0).ne(0.00)].copy()
    return sin_df, con_df


def _insert_batches(cur, sql: str, rows: list[tuple]):
    for i in range(0, len(rows), BATCH_SIZE):
        cur.executemany(sql, rows[i:i+BATCH_SIZE])


def procesar_y_cargar():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, schema=DB_NAME)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Crear tablas destino
    cur.execute(DDL_CORRECTO)
    cur.execute(DDL_ERRORES)
    conn.commit()

    # Construir dataframes
    sin_df, con_df = _build_dataframes()

    if sin_df.empty and con_df.empty:
        print("No hay datos para procesar.")
        cur.close()
        conn.close()
        return

    # Cargar SIN diferencias -> retenciones_correcto (sin columna diferencias)
    if not sin_df.empty:
        rows_sin = list(
            sin_df[[
                "no_secuencial_retencion", "id_cliente", "fecha_emision",
                "cod_retencion", "base_imponible", "monto_retencion", "porcentaje_ret"
            ]].itertuples(index=False, name=None)
        )
        _insert_batches(cur, UPSERT_CORRECTO, rows_sin)
        conn.commit()
        print(f"retenciones_correcto: upserts = {len(rows_sin)}")
    else:
        print("sin_df vacío (no hay registros correctos).")

    # Cargar CON diferencias -> retenciones_errores (incluye diferencias)
    if not con_df.empty:
        rows_con = list(
            con_df[[
                "no_secuencial_retencion", "id_cliente", "fecha_emision",
                "cod_retencion", "base_imponible", "monto_retencion",
                "porcentaje_ret", "diferencias"
            ]].itertuples(index=False, name=None)
        )
        _insert_batches(cur, UPSERT_ERRORES, rows_con)
        conn.commit()
        print(f"retenciones_errores: upserts = {len(rows_con)}")
    else:
        print("con_df vacío (no hay registros con diferencias).")

    cur.close()
    conn.close()


with DAG(
    dag_id="retenciones_control_y_carga",
    description="Lee retenciones, calcula porcentaje_ret y diferencias, separa y carga en tablas destino.",
    start_date=datetime(2025, 1, 1),
    schedule=None,      # pon @daily / cron si lo quieres periódico
    catchup=False,
    tags=["retenciones", "limpieza", "carga", "mysql", "pandas"],
) as dag:

    start = EmptyOperator(task_id="start")

    # (Opcional) valida que existan tablas base (si tu tabla 'retenciones' ya existe, puedes omitir)
    # Aquí solo aseguramos las tablas destino en el paso de carga

    run_etl = PythonOperator(
        task_id="procesar_y_cargar",
        python_callable=procesar_y_cargar,
    )

    end = EmptyOperator(task_id="end")

    start >> run_etl >> end
