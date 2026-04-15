"""
DAG: stackoverflow_lakehouse_pipeline
─────────────────────────────────────
Pipeline Medallion:
  Tarea 1 (Bronze): Ingesta users_2021 desde S3 público de ClickHouse
  Tarea 2 (Silver): Consolidar users_hist (PySpark + Iceberg)
  Tarea 3 (Gold):   Generar post_counts_by_user (PySpark + Iceberg)

Usa PythonOperator. Spark en modo CLIENT contra spark-master:7077.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import subprocess
import sys

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SPARK_MASTER = "spark://spark-master:7077"
SCRIPTS_DIR = "/opt/airflow/scripts"

JARS = ",".join([
    "/opt/spark-jars/hadoop-aws-3.3.4.jar",
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark-jars/bundle-2.24.8.jar",
    "/opt/spark-jars/url-connection-client-2.24.8.jar",
    "/opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
    "/opt/spark-jars/nessie-spark-extensions-3.5_2.12-0.77.1.jar",
])

# Configs que se pasan a spark-submit via --conf
SPARK_CONFS = {
    # Driver corre en airflow-worker (client mode)
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.memory": "2g",
    # Executor en el cluster - 1 solo, 2g, sin dynamic allocation
    "spark.executor.instances": "1",
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.cores": "1",
    "spark.executor.memory": "2g",
    "spark.executor.memoryOverhead": "512m",
    # Optimizaciones para poca RAM
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.codegen.wholeStage": "false",
    "spark.sql.codegen.factoryMode": "NO_CODEGEN",
    # Iceberg + Nessie
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    "spark.sql.defaultCatalog": "nessie",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
    "spark.sql.catalog.nessie.ref": "main",
    "spark.sql.catalog.nessie.authentication.type": "NONE",
    "spark.sql.catalog.nessie.warehouse": "s3a://warehouse/",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.sql.catalog.nessie.s3.region": "us-east-1",
    "spark.sql.catalog.nessie.s3.access-key-id": "admin",
    "spark.sql.catalog.nessie.s3.secret-access-key": "password",
    # Hadoop S3A
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

SPARK_ENV = {
    "CATALOG_URI": "http://nessie:19120/api/v1",
    "WAREHOUSE": "s3a://warehouse/",
    "STORAGE_URI": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "password",
    "AWS_REGION": "us-east-1",
    "AWS_DEFAULT_REGION": "us-east-1",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": os.environ.get("PATH", ""),
    "PYTHONUNBUFFERED": "1",
}


def _spark_submit(script_name: str):
    """Lanza spark-submit en modo CLIENT contra el cluster Spark."""
    script_path = os.path.join(SCRIPTS_DIR, script_name)

    cmd = [
        "/home/airflow/.local/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--deploy-mode", "client",
        "--jars", JARS,
    ]
    for k, v in SPARK_CONFS.items():
        cmd.extend(["--conf", f"{k}={v}"])
    cmd.append(script_path)

    print(f"Ejecutando: spark-submit --master {SPARK_MASTER} ... {script_name}")
    result = subprocess.run(
        cmd,
        env={**os.environ, **SPARK_ENV},
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"spark-submit falló con código {result.returncode}")


def bronze_task(**kwargs):
    """Tarea 1 — Bronze: Ingesta users_2021."""
    print("\n" + "=" * 60)
    print("  TAREA 1: BRONZE — Ingesta users_2021")
    print("=" * 60 + "\n")
    sys.path.insert(0, SCRIPTS_DIR)
    from bronze_ingest_users import run as bronze_run
    bronze_run(year="2021", limit=50000)


def silver_task(**kwargs):
    """Tarea 2 — Silver: Consolidar users_hist (spark-submit client)."""
    print("\n" + "=" * 60)
    print("  TAREA 2: SILVER — users_hist (spark-submit client mode)")
    print("=" * 60 + "\n")
    _spark_submit("silver_transform_users.py")


def gold_task(**kwargs):
    """Tarea 3 — Gold: post_counts_by_user (spark-submit client)."""
    print("\n" + "=" * 60)
    print("  TAREA 3: GOLD — post_counts_by_user (spark-submit client mode)")
    print("=" * 60 + "\n")
    _spark_submit("gold_aggregation.py")


with DAG(
    dag_id="stackoverflow_lakehouse_pipeline",
    description="Pipeline Medallion: Bronze → Silver → Gold (Spark cluster)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "medallion", "spark", "iceberg", "nessie", "stackoverflow"],
) as dag:

    t_bronze = PythonOperator(
        task_id="bronze_ingest_users_2021",
        python_callable=bronze_task,
    )
    t_silver = PythonOperator(
        task_id="silver_transform_users_hist",
        python_callable=silver_task,
    )
    t_gold = PythonOperator(
        task_id="gold_aggregation_post_counts",
        python_callable=gold_task,
    )

    t_bronze >> t_silver >> t_gold
