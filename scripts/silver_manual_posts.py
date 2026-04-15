"""
silver_manual_posts.py
──────────────────────
Script manual: construye post_hist en Silver leyendo ambos Parquet de Bronze.
Este NO pasa por el pipeline de Airflow.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

CATALOG_URI = os.getenv("CATALOG_URI", "http://nessie:19120/api/v1")
WAREHOUSE = os.getenv("WAREHOUSE", "s3a://warehouse/")
STORAGE_URI = os.getenv("STORAGE_URI", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

TARGET_TABLE = "nessie.silver.posts_hist"


def run():
    spark = (
        SparkSession.builder
        .appName("silver_manual_posts")
        .config("spark.sql.catalog.nessie.uri", CATALOG_URI)
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
        .config("spark.sql.catalog.nessie.s3.access-key-id", AWS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", AWS_SECRET)
        .config("spark.hadoop.fs.s3a.endpoint", STORAGE_URI)
        .config("spark.hadoop.fs.s3a.access.key", AWS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET)
        .getOrCreate()
    )

    print(f"\n{'='*60}")
    print(f"  SILVER MANUAL — posts_hist")
    print(f"{'='*60}")

    # Leer ambos años
    print("\n[1/3] Leyendo Bronze posts 2020 + 2021 ...")
    df_2020 = spark.read.parquet("s3a://bronze/posts/2020/posts_2020.parquet")
    df_2021 = spark.read.parquet("s3a://bronze/posts/2021/posts_2021.parquet")

    df_all = df_2020.unionByName(df_2021, allowMissingColumns=True)
    df_all = df_all.withColumn("fecha_cargue", current_timestamp())
    total = df_all.count()
    print(f"   Total filas combinadas: {total}")

    # Crear namespace
    print("\n[2/3] Creando namespace silver ...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    # MERGE o CREATE
    print("\n[3/3] Escribiendo tabla Iceberg ...")
    try:
        spark.sql(f"SELECT 1 FROM {TARGET_TABLE} LIMIT 1")
        df_all.createOrReplaceTempView("posts_incoming")
        spark.sql(f"""
            MERGE INTO {TARGET_TABLE} AS target
            USING posts_incoming AS source
            ON target.Id = source.Id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        final = spark.sql(f"SELECT COUNT(*) as cnt FROM {TARGET_TABLE}").collect()[0]["cnt"]
        print(f"   ✅ MERGE completado: {TARGET_TABLE} ({final} filas)")
    except Exception:
        df_all.writeTo(TARGET_TABLE).create()
        print(f"   ✅ Tabla creada: {TARGET_TABLE} ({total} filas)")

    spark.stop()
    print("\n  ✅ Silver manual posts_hist completado.\n")


if __name__ == "__main__":
    run()
