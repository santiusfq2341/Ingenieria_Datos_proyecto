# dags/crear_tabla_prueba_mysql.py
from datetime import datetime
from airflow import DAG

# Usaremos el MySqlOperator del provider
from airflow.providers.mysql.operators.mysql import MySqlOperator

# --- IMPORTANTE ---
# Define una conexión en Airflow con ID "mysql_docker".
# O bien exporta en los servicios de Airflow la variable:
# AIRFLOW_CONN_MYSQL_DOCKER='mysql+mysqlconnector://root:miclave@mysql:3306/airflow'
# (El esquema en la URI puede ser cualquier BD existente; aquí usamos "airflow".)

with DAG(
    dag_id="crear_tabla_prueba_mysql",
    description="Crea BD 'pruebas' y tabla 'tabla_prueba' con columna 'prueba' (3 dígitos).",
    start_date=datetime(2025, 1, 1),
    schedule=None,       # ejecútalo manualmente desde la UI
    catchup=False,
    tags=["mysql", "init"],
) as dag:

    crear_bd = MySqlOperator(
        task_id="crear_base_pruebas",
        mysql_conn_id="mysql_docker",
        sql="""
        CREATE DATABASE IF NOT EXISTS pruebas
        /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */;
        """,
    )

    crear_tabla = MySqlOperator(
        task_id="crear_tabla_prueba",
        mysql_conn_id="mysql_docker",
        sql="""
        CREATE TABLE IF NOT EXISTS pruebas.tabla_prueba (
            -- Se usa INT y se fuerza a 3 dígitos con un CHECK (MySQL 8+)
            prueba INT NOT NULL,
            CONSTRAINT chk_prueba_3dig CHECK (prueba BETWEEN 0 AND 999)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """,
    )

    crear_bd >> crear_tabla
