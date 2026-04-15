from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("cleanup").getOrCreate()
for t in ["nessie.gold.post_counts_by_user", "nessie.silver.posts_hist", "nessie.silver.users_hist"]:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print(f"DROPPED: {t}")
    except Exception as e:
        print(f"SKIP {t}: {e}")
spark.stop()
print("Cleanup done")
