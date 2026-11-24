from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("09-branches-tags").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Create a tag (fixed name for the current snapshot) ===")
spark.sql("ALTER TABLE local.sales.customers CREATE TAG before_test")

print("\n=== Create a branch and write to it ===")
spark.sql("ALTER TABLE local.sales.customers CREATE BRANCH audit")
spark.sql("INSERT INTO local.sales.customers.branch_audit VALUES (9, 'Test', 'Nowhere')")

print("\n=== Branch 'audit' (has the extra row) ===")
spark.sql("SELECT * FROM local.sales.customers.branch_audit ORDER BY customer_id").show()

print("\n=== Main table (no extra row) ===")
spark.sql("SELECT * FROM local.sales.customers ORDER BY customer_id").show()

print("\n=== Tag 'before_test' (table as it was when the tag was created) ===")
spark.sql("SELECT * FROM local.sales.customers VERSION AS OF 'before_test' ORDER BY customer_id").show()

spark.stop()
