"""
silver_transform_users.py
─────────────────────────
Silver: lee Bronze users 2020+2021, construye users_hist en Iceberg.
Usa merge manual (left_anti + overwritePartitions).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

CATALOG_URI  = os.getenv("CATALOG_URI",          "http://nessie:19120/api/v1")
WAREHOUSE    = os.getenv("WAREHOUSE",             "s3a://warehouse/")
STORAGE_URI  = os.getenv("STORAGE_URI",           "http://minio:9000")
AWS_KEY      = os.getenv("AWS_ACCESS_KEY_ID",     "admin")
AWS_SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

TARGET_TABLE = "nessie.silver.users_hist"


def get_spark():
    return (
        SparkSession.builder
        .appName("silver_transform_users")
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


def merge_manual(spark, df_new, table_name, key_col="Id"):
    if not table_exists(spark, table_name):
        print(f"   Tabla no existe → creando {table_name} ...")
        df_new.writeTo(table_name).using("iceberg").tableProperty("format-version", "2").create()
        print(f"   Tabla creada.")
        return

    print(f"   Tabla existe → merge manual ...")
    df_existing = spark.read.format("iceberg").load(table_name)
    df_unchanged = df_existing.join(df_new.select(key_col), on=key_col, how="left_anti")
    df_merged = df_unchanged.unionByName(df_new, allowMissingColumns=True)
    df_merged.writeTo(table_name).using("iceberg").overwritePartitions()
    print(f"   Merge completado.")


def run(**kwargs):
    spark = get_spark()
    print(f"Spark: {spark.version} | Master: {spark.sparkContext.master}")

    print("\n" + "=" * 60)
    print("  SILVER — users_hist")
    print("=" * 60)

    print("\n[1/3] Leyendo Bronze users 2020 + 2021 ...")
    df_2020 = spark.read.parquet("s3a://bronze/users/2020/users_2020.parquet")
    df_2021 = spark.read.parquet("s3a://bronze/users/2021/users_2021.parquet")
    df_all = df_2020.unionByName(df_2021, allowMissingColumns=True)
    df_all = df_all.withColumn("fecha_cargue", current_timestamp())

    print("\n[2/3] Namespace silver ...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    print("\n[3/3] Escribiendo users_hist ...")
    merge_manual(spark, df_all, TARGET_TABLE, key_col="Id")

    cnt = spark.read.format("iceberg").load(TARGET_TABLE).count()
    print(f"\n   users_hist: {cnt} filas")

    spark.stop()
    print("\n  Silver users_hist completado.\n")


if __name__ == "__main__":
    run()
