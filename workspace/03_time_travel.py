from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("03-time-travel").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Value of order 1 at every snapshot (row history) ===")
snaps = spark.sql("""
SELECT snapshot_id, operation FROM local.sales.orders.snapshots ORDER BY committed_at
""").collect()

for s in snaps:
    print(f"\nsnapshot {s.snapshot_id} ({s.operation})")
    spark.sql(f"""
        SELECT order_id, amount FROM local.sales.orders
        VERSION AS OF {s.snapshot_id}
        WHERE order_id = 1
    """).show()

print("\n=== Current table ===")
spark.sql("SELECT * FROM local.sales.orders ORDER BY order_id").show()

spark.stop()
