from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("04-schema-evolution").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Add a column (metadata-only commit, no data rewritten) ===")
spark.sql("ALTER TABLE local.sales.orders ADD COLUMN region STRING")

print("\n=== Insert a row that uses the new column ===")
spark.sql("""
INSERT INTO local.sales.orders VALUES
  (4, 'Kiran', 120.0, DATE '2026-01-13', 'South')
""")

print("\n=== Rows: old rows show NULL for region ===")
spark.sql("SELECT * FROM local.sales.orders ORDER BY order_id").show(truncate=False)

print("\n=== Data files: old files are untouched ===")
spark.sql("SELECT file_path, record_count FROM local.sales.orders.files").show(truncate=False)

spark.stop()
