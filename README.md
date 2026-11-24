# Apache Iceberg Lab (Spark + Docker)

A hands-on lab that walks through Apache Iceberg from the basics to maintenance. You run the scripts in order and read what each one prints. This README explains each step: what it does, why it matters, and what you should see.

---

## 1. Setup

### 1.1 Folder layout

```
Iceberg/
├── Dockerfile              Spark 3.5.1 image with the Iceberg 1.9.2 runtime jar baked in
├── spark-defaults.conf     Iceberg catalog settings, loaded by every Spark session
├── docker-compose.yml      One container: iceberg-spark
├── workspace/              Lab scripts (mounted into the container at /workspace)
│   ├── 00_setup_check.py
│   ├── 01_create_and_insert.py
│   ├── 02_update_delete.py
│   ├── 03_time_travel.py
│   ├── 04_schema_evolution.py
│   ├── 05_partitioning.py
│   ├── 06_small_files.py
│   ├── 07_merge_and_cdc.py
│   ├── 08_maintenance.py
│   ├── 09_branches_tags.py
│   └── reset.py
└── warehouse/              Iceberg table files (created by Spark; git-ignored)
```

### 1.2 What the Docker setup does

- **Dockerfile:** starts from the official `apache/spark:3.5.1` image and downloads the Iceberg Spark runtime jar (`iceberg-spark-runtime-3.5_2.12-1.9.2`) into Spark's jars folder. This adds Iceberg support to Spark.
- **spark-defaults.conf:** registers Iceberg's SQL extensions and a catalog named `local`. The catalog is type `hadoop`, which stores tables as folders under `/opt/iceberg/warehouse`. Because this file is loaded automatically, you never configure Spark by hand.
- **docker-compose.yml:** runs the container and mounts two folders from your Mac:
  - `workspace/` → `/workspace` (your scripts)
  - `warehouse/` → `/opt/iceberg/warehouse` (where Iceberg writes table files, so you can see them on your Mac)

### 1.3 Prerequisites

- Docker Desktop installed and running.
- If `docker` says "command not found" in Terminal, run this once and open a new Terminal window:

```bash
echo 'export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"' >> ~/.zshrc
```

### 1.4 Start the environment

```bash
cd /Users/gauravrawat/Downloads/aws_project/Iceberg
docker compose build
docker compose up -d
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/00_setup_check.py
```

- **What it does:** builds the image, starts the container, and runs a check that lists namespaces in the `local` catalog.
- **What you should see:** an empty namespace list on a fresh setup. If you see an error about the catalog, check `spark-defaults.conf`.

---

## 2. Running the lab

Run the scripts in order. Scripts 02 to 09 depend on tables created by earlier scripts.

```bash
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/01_create_and_insert.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/02_update_delete.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/03_time_travel.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/04_schema_evolution.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/05_partitioning.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/06_small_files.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/07_merge_and_cdc.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/08_maintenance.py
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/09_branches_tags.py
```

**Repeat runs:** each script uses `CREATE ... IF NOT EXISTS` and `INSERT`. Running a script twice adds its rows twice. For a clean run, reset first:

```bash
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/reset.py
```

`reset.py` drops every lab table and purges its files.

**Interactive shell (optional):**

```bash
docker exec -it iceberg-spark /opt/spark/bin/pyspark
```

**Look at the files on your Mac:**

```bash
find warehouse -type f
```

---

## 3. Step-by-step: what each script does

### 00 — Catalog and namespaces (`00_setup_check.py`)

- **What it does:** runs `SHOW NAMESPACES IN local`.
- **Why:** confirms Spark can reach the Iceberg catalog.
- **Concept:** a table's full name has three parts: `catalog.namespace.table`. In `local.sales.orders`:
  - **Catalog** (`local`): the top-level connection. Defined in `spark-defaults.conf`.
  - **Namespace** (`sales`): a folder-like group of related tables, similar to a database or schema.
  - **Table** (`orders`): the Iceberg table.

### 01 — Create a table, insert rows, see the metadata (`01_create_and_insert.py`)

- **What it does:** creates `local.sales.orders`, inserts 2 rows in one statement, then prints the rows, the snapshots, and the data files.
- **What you should see:**
  - 2 rows (Asha and Ravi).
  - 1 snapshot, with operation `append`.
  - 2 data files, one per row, or one per Spark task. The number depends on how Spark splits the write.
- **Concepts:**
  - **Creating the table writes no data file.** `CREATE TABLE` writes only `metadata/v1.metadata.json`, which holds the schema and location.
  - **Each write creates data files.** An `INSERT` writes Parquet files in `data/`.
  - **Snapshot:** one version of the table, created by each committed write. A snapshot is *not* a row count. It records which data files make up the table at that moment, and what the write did (`operation`: `append`, `overwrite`, or `delete`). The `summary` column holds counts such as `added-records` and `total-records`.
  - **`.snapshots`** is a metadata table: one row per snapshot.
  - **`.files`** is a metadata table: one row per data file in the *current* snapshot, with `record_count` and `file_path`.

**What is on disk** (from `find warehouse -type f`):

| File | What it is |
|---|---|
| `data/*.parquet` | The rows. Created by inserts. |
| `metadata/v1.metadata.json` | The table definition, written by `CREATE TABLE`. |
| `metadata/v2.metadata.json` | The table state after the first insert. Points to the new snapshot. |
| `metadata/snap-*.avro` | The snapshot's manifest list: which manifests make up this version. |
| `metadata/*-m0.avro` | A manifest: a list of data files with column statistics (min and max values), used for file pruning. |
| `metadata/version-hint.text` | The number of the current metadata version. |
| `.crc` files | Checksums written by Hadoop. Ignore them. |

**Key idea:** Iceberg never edits a file in place. Each write adds new data files and a new metadata version, and the catalog then points to the new version. This is what makes writes atomic.

### 02 — UPDATE and DELETE (`02_update_delete.py`)

- **What it does:** updates the amount of order 1 to 300.0, deletes order 2, then prints the snapshots and data files.
- **What you should see:**
  - Snapshots: `overwrite` for the update, `delete` for the delete.
  - Order 1 shows 300.0. Order 2 is gone.
- **Concept — Copy-on-Write (the default):** an UPDATE or DELETE rewrites every data file that contains a matching row. The old file is no longer part of the current snapshot, but it stays on disk until you expire old snapshots (script 08). A rewrite creates a new data file, so `.files` changes.
- **Why `overwrite` for an update:** Iceberg replaces the files that held the old rows. The snapshot operation name reflects that.

### 03 — Time travel and row history (`03_time_travel.py`)

- **What it does:** reads order 1 at each snapshot using `VERSION AS OF <snapshot_id>`, then reads the current table.
- **What you should see:** the amount for order 1 at each snapshot. Earlier snapshots show 250.0. Later ones show 300.0 after the update. The delete snapshot still shows 300.0, because order 1 wasn't deleted.
- **Concepts:**
  - **Time travel** reads an old version of the table. It doesn't change the current table.
  - **Row history:** Iceberg doesn't have a per-row history table. To see how one row changed, read it at each snapshot, which is what this script does.
  - **Changelog view (optional):** Iceberg's procedure `system.create_changelog_view` compares snapshots and labels each row change (`INSERT`, `DELETE`, `UPDATE_BEFORE`, `UPDATE_AFTER`). It's not required and not in the scripts. It's useful for CDC.

### 04 — Schema evolution (`04_schema_evolution.py`)

- **What it does:** adds a `region` column with `ALTER TABLE ... ADD COLUMN`, inserts a row that uses it, then prints the rows and the files.
- **What you should see:** older rows show `NULL` for `region`. The new row shows `South`. The old data files are unchanged.
- **Concepts:**
  - **Schema change is a metadata commit.** It writes a new `vN.metadata.json` with the new schema. It creates no snapshot and no data file.
  - **Columns are tracked by ID, not by name.** Iceberg uses an internal ID for each column, so renaming or reordering a column doesn't require rewriting data.
  - **Supported changes:** add, drop, rename, reorder, and widen types. No rewrite is needed.

### 05 — Partitioning (`05_partitioning.py`)

- **What it does:**
  1. Creates `orders_by_month`, partitioned by `months(order_date)`.
  2. Inserts 3 rows across January and February.
  3. Shows the files, then prints a query plan for a single-day filter.
  4. Changes the partitioning to days, then inserts one more row.
- **What you should see:**
  - Two files for the month partitions (one per month, for January and February).
  - A query plan with the filter pushed into the scan (`BatchScan ... filters=... order_date = <day number>`). Iceberg skips files that don't match.
  - After the change, the new row's file is in a day partition. Old files keep their month layout.
- **Concepts:**
  - **Hidden partitioning:** you filter on `order_date`. Iceberg applies `months(order_date)` for you, so queries don't need a separate partition column.
  - **Transforms:** `months()`, `days()`, `years()`, `hours()`, `bucket(n, col)`, `truncate(n, col)`.
  - **File pruning:** the planner reads manifest statistics and partition values, then skips files that can't match the filter.
  - **Partition evolution:** `REPLACE PARTITION FIELD` changes the layout for new writes only. Old files keep their old layout, and queries still work across both.
  - **The `partition` column** in `.files` is internal. For month partitions, the value counts months since January 1970. For example, 672 is January 2026.
  - **Migrating old data:** you don't have to. To rewrite everything to the new layout, run `rewrite_data_files` (script 06 shows this).

### 06 — Small files and compaction (`06_small_files.py`)

- **What it does:**
  1. Creates `file_test` with two table properties:
     - `write.distribution-mode = hash`
     - `write.target-file-size-bytes = 134217728` (128 MB)
  2. Runs 3 single-row inserts, printing the file count after each.
  3. Runs one insert with 10 rows in one month.
  4. Runs 30 rows across three months with `hash`, then with `none`.
  5. Runs `rewrite_data_files` to compact.
- **What you should see:**
  - Each single-row insert adds 1 file.
  - The 10-row insert adds 1 file, because all rows are in one partition.
  - The 30-row insert with `hash` adds about one file per month (3). With `none`, it may add more, depending on how Spark splits the work.
  - After the rewrite, the file count goes down.
- **Concepts:**
  - **Each commit writes at least one file.** One statement is one commit. Ten separate single-row inserts create ten files.
  - **What `hash` does:** it groups rows by partition value before writing. All rows for the same partition go to the same Spark task, which writes one file for that partition. It hashes on the partition column (for example, the month), not on a column you choose. Without partitioning there is nothing to group by.
  - **Target file size:** if a partition's data goes beyond `write.target-file-size-bytes`, Iceberg writes a second file. The setting limits file size, not the count of commits.
  - **Compaction:** `rewrite_data_files` merges small files into fewer, larger ones and commits the result as a new snapshot. Run it on a schedule for production tables.
  - **Why `min-input-files = 1`:** by default, compaction skips groups with fewer than 5 small files. Setting it to 1 lets the lab compact a smaller group.
  - **Batching:** the most effective fix is to write more rows per statement. The table setting helps only when one write covers many partitions.

### 07 — MERGE, CDC, idempotent pipelines, Merge-on-Read (`07_merge_and_cdc.py`)

This script has three parts.

**Part A: MERGE (upsert).**
- **What it does:** matches incoming rows to `customers` by `customer_id`. It updates matches and inserts the rest, in one commit.
- **What you should see:** customer 1 unchanged. Customer 2 changes to Mumbai. Customer 3 (Meena) is inserted.
- **Snapshot:** one new `overwrite` snapshot for the whole MERGE, not two separate writes.

**Part B: CDC and idempotent apply.**
- **What CDC is:** Change Data Capture means applying a stream of changes (insert, update, delete) from a source to a target table. Each change row here has an `op` column: `I`, `U`, or `D`.
- **What it does:** `apply_cdc` runs a MERGE with a `WHEN MATCHED AND op = 'D' THEN DELETE` clause. It records each batch ID in `processed_batches`. If the batch ID is already there, it skips the batch.
- **What you should see:**
  - The first call applies the batch: customer 5 is inserted, customer 1 changes to Bhopal, customer 2 is deleted.
  - The second call prints `already applied` and changes nothing.
- **Idempotent pipeline:** a pipeline that gives the same result whether a batch runs once or several times. Three mechanisms make this work:
  1. **Keyed MERGE:** matches on a business key, so repeating a change updates or skips the same row.
  2. **Batch log:** records which batches were applied and skips them on a retry.
  3. **Atomic commits:** each write either fully commits or doesn't, so a crash leaves no partial data.
- **Why plain INSERT is risky:** retrying a failed plain `INSERT` adds the rows again.

**Part C: Merge-on-Read.**
- **What it does:** sets the table to Merge-on-Read for updates, deletes, and merges, then updates customer 1's city and lists the files.
- **What you should see:** a small position-delete file next to the new data file. The old data file is still there but its old row is hidden.
- **Concepts:**
  - **Copy-on-Write:** updates rewrite data files. Reads are fast; writes cost more.
  - **Merge-on-Read:** updates write a small delete file instead. Writes are cheap; reads apply the deletes, so compaction matters more.
  - **Position delete file:** says "the row at position N in data file X is deleted."
  - **`all_files`** lists data files and delete files together. The `content` column is `0` for data and `1` (position) or `2` (equality) for deletes.
  - **Clean-up:** `rewrite_position_delete_files` and `rewrite_data_files` merge the deletes into new data files. Use `min-input-files = 1` in the lab.

### 08 — Maintenance: expire snapshots and orphan files (`08_maintenance.py`)

- **What it does:**
  1. Lists the snapshots of `customers`.
  2. Expires all but the 2 most recent.
  3. Lists the snapshots again.
  4. Runs a dry run of orphan-file removal.
- **What you should see:** fewer snapshots after the expire step. The orphan dry run lists files it *would* delete, but deletes nothing.
- **Concepts:**
  - **Expire snapshots** removes old snapshots from the history. You can no longer read or roll back to them. It does not change the current data, and it doesn't delete data files by itself. Files that only the removed snapshots used become orphans.
  - **Orphan file:** a file in the table folder that no snapshot references. Common causes are expired snapshots and failed writes.
  - **`remove_orphan_files`** deletes orphans. It runs in dry-run mode in this script so you can review the list first. To delete the files, change `dry_run => true` to `dry_run => false`.
  - **`older_than`** protects recent files, in case a write is still running. The lab uses the current time, which is only safe here. In production, use a cutoff such as 7 days ago.
  - **Rollback vs expire:** rollback moves the current table back to an older snapshot (a new commit, history is kept). Expire deletes history. They are different operations.

### 09 — Branches and tags (`09_branches_tags.py`)

- **What it does:**
  1. Creates a tag `before_test` on the current snapshot.
  2. Creates a branch `audit` and writes one row to it.
  3. Prints the `audit` branch, the main table, and the tag.
- **What you should see:**
  - The `audit` branch has the extra row (customer 9).
  - The main table does not have it.
  - The tag shows the table as it was when the tag was created.
- **Concepts:**
  - **Snapshot:** a commit in the table's history.
  - **Tag:** a fixed, read-only name for one snapshot. Tags never move, and you can't write to them. Use one to mark a version you want to keep, such as "before the migration." Tags also protect their snapshot from expiration.
  - **Branch:** a named, movable line of history, separate from `main`. Writes to a branch don't change `main` until you merge them. Use a branch to test changes safely.
  - **Writing to a branch:** `INSERT INTO local.sales.customers.branch_audit ...` writes to the `audit` branch.
  - **Reading a tag or branch:** `VERSION AS OF 'before_test'` reads a tag, and `.branch_audit` reads a branch.
  - **Merging a branch** (fast-forward `main` to the branch) is not in the scripts. It's an optional next step.

---

## 4. Glossary

| Term | Meaning |
|---|---|
| **Catalog** | Top-level connection that knows where tables are. Here it is `local`, using the hadoop type. |
| **Namespace** | A group of tables, like a database or schema. Here: `sales`. |
| **Table** | An Iceberg table, such as `local.sales.orders`. |
| **Data file** | A Parquet file containing rows. |
| **Manifest** | An Avro file that lists data files with column statistics. Used for pruning. |
| **Manifest list** | An Avro file that lists the manifests for one snapshot. Named `snap-*.avro`. |
| **Metadata file** | `vN.metadata.json`: schema, partition spec, snapshot pointers. A new one is written on each commit. |
| **Version hint** | `version-hint.text`: the current metadata version number. |
| **Snapshot** | One committed version of the table, with its operation and summary. |
| **Commit** | The atomic step that makes a write visible. Each write is one commit. |
| **Time travel** | Reading an older snapshot with `VERSION AS OF`. |
| **Rollback** | Moving the current table back to an older snapshot, as a new commit. |
| **Expire snapshots** | Deleting old snapshots from history. |
| **Orphan file** | A file no snapshot references. Removed by `remove_orphan_files`. |
| **Schema evolution** | Changing columns without rewriting data. Columns are tracked by ID. |
| **Hidden partitioning** | Partitioning by a transform (for example `months(order_date)`) without a separate partition column in queries. |
| **Partition evolution** | Changing the partition layout for new writes. Old files keep their layout. |
| **Pruning** | Skipping files that can't match a query, using partition values and column statistics. |
| **Write distribution (`hash`)** | Groups rows by partition before writing, to reduce the number of files. |
| **Target file size** | The size at which Iceberg starts a new data file. |
| **Compaction** | Merging small files into larger ones (`rewrite_data_files`). |
| **Copy-on-Write** | Updates and deletes rewrite data files. |
| **Merge-on-Read** | Updates and deletes write small delete files. Readers apply them. |
| **Position delete** | A delete record pointing to a row by file and position. |
| **MERGE** | One statement that updates matching rows and inserts the rest. |
| **CDC** | Change Data Capture: applying a stream of insert, update, and delete changes. |
| **Idempotent pipeline** | A pipeline that gives the same result if it runs more than once on the same input. |
| **Tag** | A fixed name for one snapshot. Read-only. |
| **Branch** | A movable, writable line of snapshots, separate from `main`. |

**Metadata tables** (queried as `<table>.<name>`):

| Table | Shows |
|---|---|
| `.snapshots` | One row per snapshot: operation, summary, and manifest list. |
| `.files` | Data files and delete files in the current snapshot. |
| `.all_files` | Data and delete files across all snapshots. |
| `.history` | Snapshot lineage: when each snapshot became current. |

---

## 5. Reset and clean-up

```bash
# Drop all lab tables and their files
docker exec iceberg-spark /opt/spark/bin/spark-submit /workspace/reset.py

# Stop the container (keeps your files)
docker compose down

# Remove all table files (start completely fresh)
rm -rf warehouse/*
```

---

## 6. Troubleshooting

| Problem | Fix |
|---|---|
| `docker: command not found` | See 1.3. Open a new Terminal after the PATH change. |
| `Cannot connect to the Docker daemon` | Open Docker Desktop and wait until it says it's running. |
| `TABLE_OR_VIEW_NOT_FOUND` | An earlier script hasn't run. Run the scripts in order, or run `reset.py` and start from 01. |
| Container name conflict | Run `docker compose down`, then `docker compose up -d`. |
| Row counts are doubled | You ran a script twice. Run `reset.py` and start again. |

---

## 7. Notes

- **Hadoop catalog:** good for learning. It stores the table pointer in the file system, so it isn't safe for several writers at once. Production setups use a Hive, REST, Glue, or JDBC catalog, which keeps the pointer in a shared service.
- **Lab shortcuts:** scripts 08 and 09 use the current time for `older_than`. Production should use an older cutoff, such as 7 days ago.
- **Not covered by the scripts:** catalog types other than hadoop, reading the same tables from other engines (Trino, Athena, Flink), merging a branch into `main`, and scheduling maintenance.
