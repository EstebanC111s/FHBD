"""
silver_manual_posts_light.py
────────────────────────────
Script manual: construye posts_hist SIN columna Body.
Body es HTML crudo que no aporta valor analítico y causa OOM.
Se ejecuta ANTES del DAG de Airflow.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

CATALOG_URI = os.getenv("CATALOG_URI", "http://nessie:19120/api/v1")
WAREHOUSE   = os.getenv("WAREHOUSE",   "s3a://warehouse/")
STORAGE_URI = os.getenv("STORAGE_URI", "http://minio:9000")
AWS_KEY     = os.getenv("AWS_ACCESS_KEY_ID",     "admin")
AWS_SECRET  = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

TARGET_TABLE = "nessie.silver.posts_hist"

# Columnas relevantes para análisis (sin Body, sin OwnerDisplayName, sin LastEditor*)
COLS = [
    "Id", "PostTypeId", "AcceptedAnswerId", "CreationDate",
    "Score", "ViewCount", "OwnerUserId", "LastActivityDate",
    "Title", "Tags", "AnswerCount", "CommentCount",
    "FavoriteCount", "ContentLicense", "ParentId",
]


def get_spark():
    return (
        SparkSession.builder
        .appName("silver_posts_light")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "nessie")
        .config("spark.sql.catalog.nessie",                     "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl",        "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",                 CATALOG_URI)
        .config("spark.sql.catalog.nessie.ref",                 "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse",           WAREHOUSE)
        .config("spark.sql.catalog.nessie.io-impl",             "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint",         STORAGE_URI)
        .config("spark.sql.catalog.nessie.s3.path-style-access","true")
        .config("spark.sql.catalog.nessie.s3.region",           "us-east-1")
        .config("spark.sql.catalog.nessie.s3.access-key-id",    AWS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key",AWS_SECRET)
        .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",                 STORAGE_URI)
        .config("spark.hadoop.fs.s3a.access.key",               AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",               AWS_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",        "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",   "false")
        .config("spark.hadoop.fs.s3a.endpoint.region",          "us-east-1")
        .getOrCreate()
    )


def table_exists(spark, name):
    try:
        spark.read.format("iceberg").load(name).limit(1).collect()
        return True
    except Exception:
        return False


def run():
    spark = get_spark()
    print(f"\n{'='*60}")
    print(f"  SILVER — posts_hist (sin Body)")
    print(f"{'='*60}")

    print("\n[1/3] Leyendo Bronze posts 2020 + 2021 (sin Body)...")
    df_2020 = spark.read.parquet("s3a://bronze/posts/2020/posts_2020.parquet").select(*COLS)
    df_2021 = spark.read.parquet("s3a://bronze/posts/2021/posts_2021.parquet").select(*COLS)
    df_all = df_2020.unionByName(df_2021, allowMissingColumns=True)
    df_all = df_all.withColumn("fecha_cargue", current_timestamp())

    print("\n[2/3] Preparando namespace y tabla...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    if table_exists(spark, TARGET_TABLE):
        print("   Eliminando tabla vieja...")
        spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

    print("\n[3/3] Creando tabla nueva...")
    df_all.writeTo(TARGET_TABLE).using("iceberg").tableProperty("format-version", "2").create()
    cnt = spark.read.format("iceberg").load(TARGET_TABLE).count()
    print(f"   posts_hist creada: {cnt} filas (sin Body)")

    spark.stop()
    print("\n  Completado.\n")


if __name__ == "__main__":
    run()
