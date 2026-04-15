"""
Register existing parquet files in Nessie Iceberg catalog
"""
import os
from pyspark.sql import SparkSession

CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3a://warehouse/"
STORAGE_URI = "http://minio:9000"
AWS_KEY = "admin"
AWS_SECRET = "password"

spark = SparkSession.builder \
    .appName("register_iceberg_tables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "nessie") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", CATALOG_URI) \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
    .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE) \
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI) \
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
    .config("spark.sql.catalog.nessie.s3.region", "us-east-1") \
    .config("spark.sql.catalog.nessie.s3.access-key-id", AWS_KEY) \
    .config("spark.sql.catalog.nessie.s3.secret-access-key", AWS_SECRET) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", STORAGE_URI) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("\n" + "="*60)
print("  REGISTERING TABLES IN NESSIE")
print("="*60)

# Create silver namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

# Register posts_hist
print("\n[1/2] Registering silver.posts_hist...")
df_posts = spark.read.parquet("s3a://bronze/posts/2020/posts_2020.parquet").unionByName(
    spark.read.parquet("s3a://bronze/posts/2021/posts_2021.parquet"), 
    allowMissingColumns=True
)
df_posts.writeTo("nessie.silver.posts_hist").using("iceberg").tableProperty("format-version", "2").create()
print(f"  ✅ posts_hist: {df_posts.count()} filas")

# Register users_hist
print("\n[2/2] Registering silver.users_hist...")
df_users = spark.read.parquet("s3a://bronze/users/2020/users_2020.parquet").unionByName(
    spark.read.parquet("s3a://bronze/users/2021/users_2021.parquet") if True else spark.createDataFrame([]),
    allowMissingColumns=True
)
df_users.writeTo("nessie.silver.users_hist").using("iceberg").tableProperty("format-version", "2").create()
print(f"  ✅ users_hist: {df_users.count()} filas")

print("\n  Completado.\n")
spark.stop()
