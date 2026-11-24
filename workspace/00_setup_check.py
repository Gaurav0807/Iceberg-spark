from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("00-setup-check").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Catalog check: namespaces in 'local' catalog ===")
spark.sql("SHOW NAMESPACES IN local").show()

spark.stop()
