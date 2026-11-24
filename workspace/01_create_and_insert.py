from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("01-create-and-insert").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Create namespace and table ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.sales")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.sales.orders (
    order_id INT,
    customer STRING,
    amount DOUBLE,
    order_date DATE
) USING iceberg
""")

print("\n=== Insert two rows (one commit, one data file) ===")
spark.sql("""
INSERT INTO local.sales.orders VALUES
  (1, 'Asha', 250.0, DATE '2026-01-10'),
  (2, 'Ravi', 90.5, DATE '2026-01-11')
""")

print("\n=== Rows ===")
spark.sql("SELECT * FROM local.sales.orders ORDER BY order_id").show()

print("\n=== Snapshots (one per commit) ===")
spark.sql("SELECT snapshot_id, operation, committed_at FROM local.sales.orders.snapshots ORDER BY committed_at").show(truncate=False)

print("\n=== Data files in the current snapshot ===")
spark.sql("SELECT file_path, record_count FROM local.sales.orders.files").show(truncate=False)

spark.stop()
