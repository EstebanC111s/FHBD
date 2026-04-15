"""
gold_aggregation.py
───────────────────
Gold: genera post_counts_by_user cruzando posts_hist y users_hist de Silver.
Usa merge manual. Lee tablas Iceberg (posts_hist ya viene sin Body).
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_date, count, avg, sum as spark_sum,
    when, lit, round as spark_round, coalesce
)

CATALOG_URI  = os.getenv("CATALOG_URI", "http://nessie:19120/api/v1")
WAREHOUSE    = os.getenv("WAREHOUSE",   "s3a://warehouse/")
STORAGE_URI  = os.getenv("STORAGE_URI", "http://minio:9000")
AWS_KEY      = os.getenv("AWS_ACCESS_KEY_ID",     "admin")
AWS_SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

SOURCE_POSTS   = "nessie.silver.posts_hist"
SOURCE_USERS   = "nessie.silver.users_hist"
GOLD_TABLE     = "nessie.gold.post_counts_by_user"
GOLD_NAMESPACE = "nessie.gold"


def get_spark():
    return (
        SparkSession.builder
        .appName("gold_aggregation")
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
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.driver.extraJavaOptions",
                "-Daws.region=us-east-1 -Daws.accessKeyId=admin -Daws.secretAccessKey=password")
        .getOrCreate()
    )


def table_exists(spark, name):
    try:
        spark.read.format("iceberg").load(name).limit(1).collect()
        return True
    except Exception:
        return False


def merge_manual(spark, df_new, table_name, key_col="OwnerUserId"):
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
    print("  GOLD — post_counts_by_user")
    print("=" * 60)

    # 1. Validar fuentes
    print("\n[1/5] Validando tablas Silver...")
    if not table_exists(spark, SOURCE_POSTS):
        raise RuntimeError(f"No existe {SOURCE_POSTS}. Ejecutar silver_manual_posts_light.py primero.")
    if not table_exists(spark, SOURCE_USERS):
        raise RuntimeError(f"No existe {SOURCE_USERS}. Ejecutar el DAG completo primero.")
    print("   Tablas encontradas.")

    # 2. Leer Silver via Iceberg
    print("\n[2/5] Leyendo Silver...")
    df_posts = spark.read.format("iceberg").load(SOURCE_POSTS)
    df_users = spark.read.format("iceberg").load(SOURCE_USERS)

    # 3. Agregar métricas por usuario
    print("\n[3/5] Calculando métricas por usuario...")
    df_agg = (
        df_posts
        .filter(col("OwnerUserId").isNotNull())
        .groupBy(col("OwnerUserId"))
        .agg(
            count("Id").alias("total_posts"),
            count(when(col("PostTypeId") == 1, True)).alias("total_preguntas"),
            count(when(col("PostTypeId") == 2, True)).alias("total_respuestas"),
            spark_sum("Score").alias("total_score"),
            spark_round(avg("Score"), 2).alias("score_promedio"),
            spark_sum("ViewCount").alias("total_views"),
            spark_sum("CommentCount").alias("total_comments"),
            spark_sum("AnswerCount").alias("total_answers_received"),
        )
    )

    # 4. Join con users
    print("\n[4/5] Cruzando con users_hist...")
    df_gold = (
        df_agg
        .join(
            df_users.select(
                col("Id").alias("UserId"),
                "DisplayName",
                col("Reputation").cast("string").cast("int").alias("Reputation"),
                "Location",
                "Views", "UpVotes", "DownVotes"
            ),
            df_agg["OwnerUserId"] == col("UserId"),
            "left"
        )
        .drop("UserId")
        .withColumn("fecha_cargue", current_date())
    )

    # 5. Escribir Gold
    print("\n[5/5] Escribiendo Gold...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    merge_manual(spark, df_gold, GOLD_TABLE, key_col="OwnerUserId")

    cnt = spark.read.format("iceberg").load(GOLD_TABLE).count()
    print(f"\n   Gold: {cnt} filas")
    spark.read.format("iceberg").load(GOLD_TABLE).show(5, truncate=False)

    spark.stop()
    print("\n  Gold completado.\n")


if __name__ == "__main__":
    run()
