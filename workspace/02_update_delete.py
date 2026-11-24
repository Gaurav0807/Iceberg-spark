from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("02-update-delete").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== UPDATE order 1 (Copy-on-Write rewrites the file) ===")
spark.sql("UPDATE local.sales.orders SET amount = 300.0 WHERE order_id = 1")
spark.sql("SELECT * FROM local.sales.orders ORDER BY order_id").show()

print("\n=== DELETE order 2 ===")
spark.sql("DELETE FROM local.sales.orders WHERE order_id = 2")
spark.sql("SELECT * FROM local.sales.orders ORDER BY order_id").show()

print("\n=== Snapshots after UPDATE and DELETE ===")
spark.sql("SELECT snapshot_id, operation FROM local.sales.orders.snapshots ORDER BY committed_at").show(truncate=False)

print("\n=== Data files now ===")
spark.sql("SELECT file_path, record_count FROM local.sales.orders.files").show(truncate=False)

spark.stop()
