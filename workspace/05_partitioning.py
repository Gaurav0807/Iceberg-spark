from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("05-partitioning").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Create table partitioned by month (hidden partitioning) ===")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.sales.orders_by_month (
    order_id INT,
    customer STRING,
    amount DOUBLE,
    order_date DATE
) USING iceberg
PARTITIONED BY (months(order_date))
""")

spark.sql("""
INSERT INTO local.sales.orders_by_month VALUES
  (1, 'Asha', 250.0, DATE '2026-01-10'),
  (2, 'Ravi', 90.5, DATE '2026-02-11'),
  (3, 'Meena', 400.0, DATE '2026-02-12')
""")

print("\n=== One data file per month ===")
spark.sql("SELECT partition, record_count, file_path FROM local.sales.orders_by_month.files").show(truncate=False)

print("\n=== Query plan for a single-day filter (look for the pushed-down filter) ===")
spark.sql("SELECT * FROM local.sales.orders_by_month WHERE order_date = DATE '2026-02-11'").explain(True)

print("\n=== Partition evolution: change month -> day (old files keep the month layout) ===")
spark.sql("ALTER TABLE local.sales.orders_by_month REPLACE PARTITION FIELD months(order_date) WITH days(order_date)")
spark.sql("""
INSERT INTO local.sales.orders_by_month VALUES
  (4, 'Kiran', 120.0, DATE '2026-02-13')
""")
spark.sql("SELECT partition, record_count, file_path FROM local.sales.orders_by_month.files").show(truncate=False)

spark.stop()
