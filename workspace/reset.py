from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("reset").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

tables = [
    "local.sales.orders",
    "local.sales.orders_by_month",
    "local.sales.file_test",
    "local.sales.customers",
    "local.sales.processed_batches",
]

for t in tables:
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    print("dropped:", t)

spark.stop()
