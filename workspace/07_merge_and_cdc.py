from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("07-merge-and-cdc").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n=== Create customers table and seed it ===")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.sales.customers (
    customer_id INT,
    name STRING,
    city STRING
) USING iceberg
""")
spark.sql("""
INSERT INTO local.sales.customers VALUES
  (1, 'Asha', 'Pune'),
  (2, 'Ravi', 'Delhi')
""")

print("\n=== MERGE: update matched rows, insert new ones (one commit) ===")
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_updates AS
SELECT * FROM VALUES
  (2, 'Ravi', 'Mumbai'),
  (3, 'Meena', 'Chennai')
AS t(customer_id, name, city)
""")
spark.sql("""
MERGE INTO local.sales.customers t
USING customer_updates s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.city = s.city
WHEN NOT MATCHED THEN INSERT (customer_id, name, city) VALUES (s.customer_id, s.name, s.city)
""")
spark.sql("SELECT * FROM local.sales.customers ORDER BY customer_id").show()

print("\n=== Idempotent CDC: batch log + MERGE with insert/update/delete ===")
spark.sql("CREATE TABLE IF NOT EXISTS local.sales.processed_batches (batch_id STRING) USING iceberg")


def apply_cdc(batch_id, rows_sql):
    done = spark.sql(
        f"SELECT count(*) AS c FROM local.sales.processed_batches WHERE batch_id = '{batch_id}'"
    ).collect()[0].c
    if done:
        print("already applied:", batch_id)
        return

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW cdc_batch AS
        SELECT * FROM VALUES {rows_sql} AS t(customer_id, name, city, op)
    """)
    spark.sql("""
        MERGE INTO local.sales.customers t
        USING cdc_batch s
        ON t.customer_id = s.customer_id
        WHEN MATCHED AND s.op = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.name = s.name, t.city = s.city
        WHEN NOT MATCHED AND s.op != 'D' THEN INSERT (customer_id, name, city) VALUES (s.customer_id, s.name, s.city)
    """)
    spark.sql(f"INSERT INTO local.sales.processed_batches VALUES ('{batch_id}')")
    print("applied:", batch_id)


changes = "(5, 'Dev', 'Pune', 'I'), (1, 'Asha', 'Bhopal', 'U'), (2, 'Ravi', 'Mumbai', 'D')"
apply_cdc("batch-001", changes)
apply_cdc("batch-001", changes)  # second run is skipped
spark.sql("SELECT * FROM local.sales.customers ORDER BY customer_id").show()

print("\n=== Merge-on-Read: updates write delete files instead of rewriting data ===")
spark.sql("""
ALTER TABLE local.sales.customers SET TBLPROPERTIES (
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
)
""")
spark.sql("UPDATE local.sales.customers SET city = 'Kolkata' WHERE customer_id = 1")
spark.sql("SELECT content, file_path, record_count FROM local.sales.customers.all_files ORDER BY file_path").show(truncate=False)
spark.sql("SELECT * FROM local.sales.customers ORDER BY customer_id").show()

spark.stop()
