from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("06-small-files").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def file_count():
    return spark.sql("SELECT count(*) AS c FROM local.sales.file_test.files").collect()[0].c


print("\n=== Create table with write-distribution and target file size ===")
spark.sql("""
CREATE TABLE IF NOT EXISTS local.sales.file_test (
    order_id INT,
    customer STRING,
    amount DOUBLE,
    order_date DATE
) USING iceberg
PARTITIONED BY (months(order_date))
TBLPROPERTIES (
    'write.distribution-mode' = 'hash',
    'write.target-file-size-bytes' = '134217728'
)
""")
print("start file count:", file_count())

print("\n=== 3 single-row inserts: each one adds one file ===")
for i in range(3):
    spark.sql(f"INSERT INTO local.sales.file_test VALUES ({i}, 'single{i}', 10.0, DATE '2026-03-01')")
    print(f"after single insert {i}: {file_count()}")

print("\n=== 1 insert with 10 rows in one month: adds one file ===")
values = ", ".join(f"({100+i}, 'batch{i}', 20.0, DATE '2026-03-01')" for i in range(10))
before = file_count()
spark.sql(f"INSERT INTO local.sales.file_test VALUES {values}")
print("files added:", file_count() - before)

print("\n=== 30 rows across 3 months with hash distribution ===")
values = ", ".join(f"({200+i}, 'mix{i}', 30.0, DATE '2026-{(i % 3) + 4:02d}-01')" for i in range(30))
before = file_count()
spark.sql(f"INSERT INTO local.sales.file_test VALUES {values}")
print("files added with hash:", file_count() - before)

print("\n=== Same 30 rows with distribution set to none ===")
spark.sql("ALTER TABLE local.sales.file_test SET TBLPROPERTIES ('write.distribution-mode' = 'none')")
values2 = values.replace("mix", "mixB")
before = file_count()
spark.sql(f"INSERT INTO local.sales.file_test VALUES {values2}")
print("files added with none:", file_count() - before)

print("\n=== Compact small files ===")
print("before rewrite:", file_count())
spark.sql("""
CALL local.system.rewrite_data_files(
    table => 'local.sales.file_test',
    options => map('min-input-files', '1')
)
""").show(truncate=False)
print("after rewrite:", file_count())

spark.stop()
