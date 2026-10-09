# Serverless Compatibility Fixes — Section 3: Incremental Data Processing

This document records all changes made to adapt the course notebooks for a
Unity Catalog-only Serverless workspace. The original notebooks were written for
classic clusters with DBFS mounts and unrestricted Spark configuration.

Covers three notebooks:
- **3.1 - Structured Streaming**
- **3.2 - Auto Loader**
- **3.3 - Multi-Hop Architecture**

## Environment

- **Compute:** Serverless interactive (Spark Connect)
- **Catalog:** Unity Catalog (`workspace` catalog)
- **Cloud provider:** AWS
- **Key constraints:**
  - DBFS root and mount points (`dbfs:/mnt/...`) are disabled
  - Custom Spark config keys (e.g., `dataset.bookstore`) cannot be set via `spark.conf.set` or SQL `SET` — returns `CONFIG_NOT_AVAILABLE`
  - `${param}` substitution in SQL cells reads from Spark config, not from Databricks widgets
  - `input_file_name()` is not supported in Unity Catalog
  - Streaming checkpoint locations must use UC volume paths, not `dbfs:/mnt/...`

---

## UC Volume (reused from Section 2)

The managed Unity Catalog volume `workspace.default.bookstore_data` was created
in Section 2. All three Section 3 notebooks reuse this volume for:
- Dataset files (orders-raw, orders-streaming, customers-json, etc.)
- Streaming checkpoint locations (`/Volumes/workspace/default/bookstore_data/checkpoints/...`)

---

## Notebook 3.1 — Structured Streaming

### Cell 2 — Setup: Copy-Datasets to UC Volume

**Original:**
```
%run ../Includes/Copy-Datasets
```

**Root cause:** The `Includes/Copy-Datasets` notebook calls
`spark.conf.set("dataset.bookstore", ...)` which fails on Serverless Spark Connect
with `CONFIG_NOT_AVAILABLE`. Additionally, `dbfs:/mnt/demo-datasets/bookstore`
is disabled on Serverless (`DBFS_DISABLED`).

**Fix:** Replaced `%run` with inline Python that:
1. Sets `dataset_bookstore` to the UC volume path
2. Copies data from S3 to the volume on first run (idempotent check)
3. Prints a note that `spark.conf.set` is not available on Serverless

**Variable:** `dataset_bookstore = '/Volumes/workspace/default/bookstore_data'`

### Cells 6, 8, 10, 12 — Streaming queries via SQL require explicit checkpoint

**Original:** SQL cells queried the streaming temp view `books_streaming_tmp_vw`
without a checkpoint location:
```sql
SELECT * FROM books_streaming_tmp_vw                          -- Cell 6
SELECT author, count(book_id) AS total_books                   -- Cell 8
  FROM books_streaming_tmp_vw GROUP BY author
SELECT * FROM books_streaming_tmp_vw ORDER BY author           -- Cell 10
CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (...)      -- Cell 12
  FROM books_streaming_tmp_vw GROUP BY author
```

**Root cause:** On Serverless, querying a streaming temp view via SQL requires
an explicit checkpoint location (`TEMP_CHECKPOINT_LOCATION_NOT_SUPPORTED`).
SQL syntax has no way to specify a checkpoint — only `display(df, checkpointLocation=...)`
in Python or `.option("checkpointLocation", ...)` in writeStream support it.
Additionally, `display()` on non-aggregated streaming DataFrames defaults to
"complete" output mode, which is not supported without aggregations
(`STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`). The `outputMode` parameter
to `display()` is not respected on Spark Connect, and `foreachBatch` cannot
serialize functions referencing the Spark session.

**Fix:** Converted all four cells from SQL to Python using `writeStream` with
explicit `outputMode`, `trigger(availableNow=True)`, and `.table()` sink, then
`display()` on the resulting Delta table:
- **Cell 6:** `writeStream.outputMode("append").trigger(availableNow=True).table("books_streaming_display")`
- **Cell 8:** `writeStream.outputMode("complete").trigger(availableNow=True).table("books_streaming_agg")`
  (complete mode is valid here because it has a GROUP BY aggregation)
- **Cell 10:** `writeStream.outputMode("append").trigger(availableNow=True).table("books_streaming_orderby")`
  — still errors with "Sorting is not supported on streaming DataFrames/Datasets",
  which is the intended teaching point of the "Unsupported Operations" section.
- **Cell 12:** `spark.table("books_streaming_tmp_vw").groupBy(...).createOrReplaceTempView(...)`
  — creates the streaming temp view via PySpark API instead of SQL DDL.

### Cell 13 — `processingTime` trigger not supported on Serverless

**Original:**
```python
.trigger(processingTime='4 seconds')
```

**Root cause:** `ProcessingTime` triggers (continuous streaming) are not
supported on Serverless compute (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`).
The error message recommends using `AvailableNow` or `Once` triggers instead.

**Fix:** Changed `trigger(processingTime='4 seconds')` to
`trigger(availableNow=True)` and added `.awaitTermination()` to wait for the
batch processing to complete. `availableNow=True` processes all available
data in one batch and stops, which is the Serverless-compatible alternative
to continuous streaming.

### Cell 19 — Streaming checkpoint location (`dbfs:/mnt/demo/...`)

**Original:**
```python
.option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
```

**Root cause:** DBFS mount points (`dbfs:/mnt/...`) are disabled on Serverless
(`DBFS_DISABLED`). Streaming checkpoint locations must use UC-compatible paths.

**Fix:** Replaced `dbfs:/mnt/demo/author_counts_checkpoint` with
`/Volumes/workspace/default/bookstore_data/checkpoints/author_counts`.

---

## Notebook 3.2 — Auto Loader

### Cell 2 — Setup: Copy-Datasets to UC Volume + Helper Functions

**Original:**
```
%run ../Includes/Copy-Datasets
```

**Root cause:** Same as Notebook 3.1 Cell 2. Additionally, the Copy-Datasets
notebook defines `load_new_data()` which is used in Cell 10 to simulate
new data arriving in the streaming source directory.

**Fix:** Replaced `%run` with inline Python that:
1. Sets `dataset_bookstore` to the UC volume path
2. Copies data from S3 to the volume on first run (idempotent check)
3. Defines `get_index()`, `load_file()`, and `load_new_data()` helper functions
   that copy parquet files from `orders-streaming/` to `orders-raw/` to
   simulate streaming data arrival
4. Prints a note that `spark.conf.set` is not available on Serverless

### Cell 6 — Auto Loader checkpoint and schema location

**Original:**
```python
.option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
.option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
```

**Root cause:** DBFS mount points are disabled on Serverless.

**Fix:** Replaced both `dbfs:/mnt/demo/orders_checkpoint` with
`/Volumes/workspace/default/bookstore_data/checkpoints/orders`.
Also added `.trigger(availableNow=True)` and `.awaitTermination()` — the
default `ProcessingTime` trigger is not supported on Serverless
(`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`).

### Cell 17 — Cleanup checkpoint directory

**Original:**
```python
dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
```

**Root cause:** DBFS mount points are disabled on Serverless.

**Fix:** Replaced with
`dbutils.fs.rm("/Volumes/workspace/default/bookstore_data/checkpoints/orders", True)`.

---

## Notebook 3.3 — Multi-Hop Architecture

### Cell 2 — Setup: Copy-Datasets to UC Volume + Helper Functions

Same fix as Notebook 3.2 Cell 2: replaced `%run ../Includes/Copy-Datasets`
with inline Python that sets `dataset_bookstore` to the UC volume path and
defines `load_new_data()` and related helper functions.

### Cell 6 — Auto Loader schema location

**Original:**
```python
.option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
```

**Root cause:** DBFS mount points are disabled on Serverless.

**Fix:** Replaced with
`/Volumes/workspace/default/bookstore_data/checkpoints/orders_raw`.

### Cell 8 — `input_file_name()` and `_metadata.file_path` both unsupported via streaming temp view

**Original:**
```sql
CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
  SELECT *, current_timestamp() arrival_time, input_file_name() source_file
  FROM orders_raw_temp
)
```

**Root cause:** `input_file_name()` is not supported in Unity Catalog on
Serverless/Standard compute (`UC_COMMAND_NOT_SUPPORTED`). The initial fix
replaced it with `_metadata.file_path`, but `_metadata` is a pseudo-column
available only at the file scan level — it is NOT exposed through the
streaming temp view `orders_raw_temp` (created by `createOrReplaceTempView`
in Cell 6), resulting in `UNRESOLVED_COLUMN`.

**Fix:** Removed the `source_file` column from the SELECT entirely. The
`source_file` column is not referenced by any downstream cells. To restore
it, Cell 6 would need to include `_metadata.file_path` in the Auto Loader
DataFrame before creating the temp view.

### Cell 9 — Streaming SQL query requires explicit checkpoint

**Original:**
```sql
SELECT * FROM orders_tmp
```

**Root cause:** `orders_tmp` is a streaming temp view (derived from Auto
Loader). Querying it via SQL on Serverless requires an explicit checkpoint
location (`TEMP_CHECKPOINT_LOCATION_NOT_SUPPORTED`). SQL syntax cannot
specify a checkpoint. Additionally, `display()` on non-aggregated streams
defaults to "complete" output mode, which is unsupported on Spark Connect
(`STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`).

**Fix:** Converted the SQL cell to Python using `writeStream` with
`outputMode("append")`, `trigger(availableNow=True)`, and an explicit
checkpoint location to write to a Delta table (`orders_tmp_display`), then
`display()` on that table — same pattern as 3.1 Cells 6, 8, 10.

### Cells 11, 20, 27 — Streaming checkpoint locations and trigger

**Original:**
```python
.option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")   # Cell 11
.option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")  # Cell 20
.option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")  # Cell 27
```

**Root cause:** DBFS mount points are disabled on Serverless. Additionally,
Cells 11 and 20 used the default `ProcessingTime` trigger, which is not
supported on Serverless (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`).

**Fix:** Replaced each `dbfs:/mnt/demo/checkpoints/...` with the corresponding
UC volume path under `/Volumes/workspace/default/bookstore_data/checkpoints/`.
Also added `.trigger(availableNow=True)` and `.awaitTermination()` to
Cells 11 and 20.

---

## Summary of Serverless Limitations Encountered

| Limitation | Error Class | Affected Notebooks / Cells | Fix |
|---|---|---|---|
| Custom Spark config keys not settable | `CONFIG_NOT_AVAILABLE` | 3.1 Cell 2; 3.2 Cell 2; 3.3 Cell 2 | Inline Python variable + UC volume |
| DBFS checkpoint paths disabled | `DBFS_DISABLED` | 3.1 Cells 13, 19; 3.2 Cells 6, 17; 3.3 Cells 6, 11, 20, 27 | UC volume checkpoint paths |
| `input_file_name()` blocked | `UC_COMMAND_NOT_SUPPORTED` | 3.3 Cell 8 | Removed `source_file` column (neither `input_file_name()` nor `_metadata.file_path` works via streaming temp view) |
| `load_new_data()` not available without `%run` | `NameError` | 3.2 Cell 10; 3.3 Cells 13, 23, 29 | Inline helper function definitions |
| DBFS root disabled | `DBFS_DISABLED` | 3.1 Cell 2 (original path) | UC volume |
| Streaming SQL queries need explicit checkpoint | `TEMP_CHECKPOINT_LOCATION_NOT_SUPPORTED` | 3.1 Cells 6, 8, 10, 12; 3.3 Cell 9 | Convert to `writeStream` with explicit `outputMode` + `trigger(availableNow=True)` → Delta table, then `display()` |
| `display()` output mode not overridable on Spark Connect | `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION` | 3.1 Cells 6, 8, 10; 3.3 Cell 9 | Use `writeStream.outputMode(...)` instead of `display(df, outputMode=...)` |
| `processingTime` trigger not supported | `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | 3.1 Cell 13; 3.2 Cell 6; 3.3 Cells 11, 20 | `trigger(availableNow=True)` + `.awaitTermination()` |