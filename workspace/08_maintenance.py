from datetime import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("08-maintenance").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Current time as a timestamp string. In this lab, older_than = now lets us expire recent snapshots.
# In production, use an older time (for example, 7 days ago) so you keep recent history.
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print("\n=== Snapshots before expiration ===")
spark.sql("SELECT snapshot_id, operation FROM local.sales.customers.snapshots ORDER BY committed_at").show(truncate=False)

print("\n=== Expire snapshots, keep the last 2 ===")
spark.sql(f"""
CALL local.system.expire_snapshots(
    table => 'local.sales.customers',
    older_than => TIMESTAMP '{now}',
    retain_last => 2
)
""").show(truncate=False)

print("\n=== Snapshots after expiration ===")
spark.sql("SELECT snapshot_id, operation FROM local.sales.customers.snapshots ORDER BY committed_at").show(truncate=False)

print("\n=== Remove orphan files: DRY RUN (lists files, deletes nothing) ===")
spark.sql(f"""
CALL local.system.remove_orphan_files(
    table => 'local.sales.customers',
    older_than => TIMESTAMP '{now}',
    dry_run => true
)
""").show(truncate=False)

spark.stop()
