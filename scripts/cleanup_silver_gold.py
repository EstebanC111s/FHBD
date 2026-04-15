from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('cleanup').getOrCreate()
spark.sql('DROP TABLE IF EXISTS nessie.silver.posts_hist')
spark.sql('DROP TABLE IF EXISTS nessie.silver.users_hist')
spark.sql('DROP TABLE IF EXISTS nessie.gold.post_counts_by_user')
print('Tables dropped from Nessie')
spark.stop()
