# Serverless Compatibility Fixes — Section 2: ELT with Spark SQL and Python

This document records all changes made to adapt the course notebooks for a
Unity Catalog-only Serverless workspace. The original notebooks were written for
classic clusters with DBFS mounts and unrestricted Spark configuration.

Covers five notebooks:
- **2.1 - Querying Files**
- **2.2 - Writing to Tables**
- **2.3 - Advanced Transformations**
- **2.4 - Higher Order Functions and SQL UDFs**
- **2.5 - Data Transformations with PySpark**

## Environment

- **Compute:** Serverless interactive (Spark Connect)
- **Catalog:** Unity Catalog (`workspace` catalog)
- **Cloud provider:** AWS
- **Key constraints:**
  - DBFS root and mount points (`dbfs:/mnt/...`) are disabled
  - Custom Spark config keys (e.g., `dataset.bookstore`) cannot be set via `spark.conf.set` or SQL `SET` — returns `CONFIG_NOT_AVAILABLE`
  - `${param}` substitution in SQL cells reads from Spark config, not from Databricks widgets
  - `input_file_name()` is not supported in Unity Catalog
  - `REFRESH TABLE` is not supported on Serverless compute
  - `CREATE TABLE ... LOCATION` with `/Volumes/...` or `dbfs:/Volumes/...` paths fails (missing scheme or dbfs scheme not supported); UC only allows Delta for managed tables

---

## UC Volume Setup

### Created: `workspace.default.bookstore_data`

A managed Unity Catalog volume was created to replace the `dbfs:/mnt/demo-datasets/bookstore`
path used by the original `Includes/Copy-Datasets` notebook.

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.bookstore_data
```

The bookstore dataset was copied from the public S3 bucket to the volume:

```python
dbutils.fs.cp(
    "s3://dalhussein-courses/datasets/bookstore/v1/",
    "/Volumes/workspace/default/bookstore_data/",
    True  # recurse
)
```

The S3 bucket is directly accessible from Serverless without custom `fs.s3a.*`
configs. The volume contains 12 directories: `books-cdc`, `books-csv`,
`books-csv-new`, `books-streaming`, `customers-json`, `customers-json-new`,
`orders`, `orders-json-raw`, `orders-json-streaming`, `orders-new`, `orders-raw`,
`orders-streaming`.

---

## Notebook 2.1 — Querying Files

### Cell 3 — Setup: Copy-Datasets to UC Volume

**Original:**
```
%run ../Includes/Copy-Datasets
```

**Root cause:** The `Includes/Copy-Datasets` notebook calls
`spark.conf.set("dataset.bookstore", ...)` which fails on Serverless Spark Connect
with `CONFIG_NOT_AVAILABLE` — custom config keys are not in the allowlist.
Additionally, `dbfs:/mnt/demo-datasets/bookstore` is disabled on Serverless
(`DBFS_DISABLED`). The `%run` magic cannot be wrapped in try/except, so the
entire cell fails, leaving the `dataset_bookstore` Python variable undefined
for downstream cells.

**Fix:** Replaced `%run` with inline Python that:
1. Sets `dataset_bookstore` to the UC volume path
2. Copies data from S3 to the volume on first run (idempotent check)
3. Prints a note that `spark.conf.set` is not available on Serverless

**Variable:** `dataset_bookstore = '/Volumes/workspace/default/bookstore_data'`

---

### Cells 5–8, 11, 13, 15, 27, 28 — Path substitution (`${dataset.bookstore}`)

**Original:** SQL cells used `${dataset.bookstore}` in file paths, e.g.:
```sql
SELECT * FROM json.`${dataset.bookstore}/customers-json`
```

**Root cause:** `${param}` in SQL cells reads from Spark config. Since
`spark.conf.set("dataset.bookstore", ...)` is not available on Serverless,
the variable is never substituted. The literal string `${dataset.bookstore}`
is passed through to Spark, which prepends `dbfs:` and fails with
`PATH_NOT_FOUND`.

**Fix:** Replaced `${dataset.bookstore}` with the literal path
`/Volumes/workspace/default/bookstore_data` in all 11 SQL cells.

---

### Cell 9 — `input_file_name()` not supported

**Original:**
```sql
SELECT *,
    input_file_name() source_file
FROM json.`/Volumes/workspace/default/bookstore_data/customers-json`;
```

**Root cause:** `input_file_name()` is not supported in Unity Catalog on
Serverless/Standard compute (`UC_COMMAND_NOT_SUPPORTED`). The error message
recommends using `_metadata.file_path` instead.

**Fix:**
```sql
SELECT *,
    _metadata.file_path source_file
FROM json.`/Volumes/workspace/default/bookstore_data/customers-json`;
```

---

### Cell 16 — External CSV table creation

**Original:**
```sql
CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (header = "true", delimiter = ";")
LOCATION "/Volumes/workspace/default/bookstore_data/books-csv"
```

**Root cause:** `CREATE TABLE ... LOCATION` fails on Serverless with UC:
- `/Volumes/...` path → `Missing cloud file system scheme`
- `dbfs:/Volumes/...` → `dbfs scheme not supported for table creation`
- `s3://...` → `No parent external location` (requires a UC external location)
- CTAS with `USING CSV` → `Only Delta is supported for managed tables`

**Fix:** Two-step approach using a temp view (which bypasses UC credential
resolution) and CTAS:
```sql
CREATE OR REPLACE TEMP VIEW books_csv_tmp
USING CSV
OPTIONS (
  path = "/Volumes/workspace/default/bookstore_data/books-csv",
  header = "true",
  delimiter = ";"
);

CREATE OR REPLACE TABLE books_csv AS
SELECT
  book_id, title, author, category,
  CAST(price AS DOUBLE) AS price
FROM books_csv_tmp
```

The resulting `books_csv` table is a managed Delta table with the same schema
and data as the original external CSV table would have had.

---

### Cell 24 — `REFRESH TABLE` not supported

**Original:**
```sql
REFRESH TABLE books_csv
```

**Root cause:** `REFRESH TABLE` is not supported on Serverless compute
(`NOT_SUPPORTED_WITH_SERVERLESS`).

**Fix:** Replaced with an explanatory SELECT. Since `books_csv` is now a managed
Delta table, its metadata is always current via the Delta transaction log —
no refresh is needed.

---

### Cell 29 — Last `${dataset.bookstore}` substitution

**Original:**
```sql
CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS SELECT * FROM books_tmp_vw;

SELECT * FROM books
```

**Root cause:** Same as other SQL cells — `${dataset.bookstore}` not substituted
on Serverless, causing `PATH_NOT_FOUND`.

**Fix:** Replaced `${dataset.bookstore}` with
`/Volumes/workspace/default/bookstore_data`.

---

## Notebook 2.2 — Writing to Tables

### Cell 2 — Setup: Copy-Datasets to UC Volume

Same fix as Notebook 2.1 Cell 3: replaced `%run ../Includes/Copy-Datasets`
with inline Python that sets `dataset_bookstore` to the UC volume path.

### Cells 3, 6, 8, 10, 12, 15, 16 — Path substitution (`${dataset.bookstore}`)

Same fix as Notebook 2.1: replaced `${dataset.bookstore}` with
`/Volumes/workspace/default/bookstore_data` in all 7 SQL cells.

### Cell 3 — `TABLE_OR_VIEW_ALREADY_EXISTS`

**Original:**
```sql
CREATE TABLE orders AS
SELECT * FROM parquet.`/Volumes/workspace/default/bookstore_data/orders`
```

**Root cause:** The `orders` table already existed from a previous run.
`CREATE TABLE` (without `OR REPLACE`) fails when the table already exists.

**Fix:** Changed to `CREATE OR REPLACE TABLE orders AS` to make the statement
idempotent.

---

## Notebook 2.3 — Advanced Transformations

### Cell 2 — Setup: Copy-Datasets to UC Volume

Same fix as Notebook 2.1 Cell 3: replaced `%run ../Includes/Copy-Datasets`
with inline Python that sets `dataset_bookstore` to the UC volume path.

### Cell 23 — Path substitution (`${dataset.bookstore}`)

**Original:**
```sql
CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;
```

**Root cause:** Same as other SQL cells — `${dataset.bookstore}` not substituted
on Serverless, causing `PATH_NOT_FOUND`.

**Fix:** Replaced `${dataset.bookstore}` with
`/Volumes/workspace/default/bookstore_data`.

### Cell 8 — `from_json` with invalid schema argument

**Original:**
```sql
SELECT from_json(profile, 'schema') AS profile_struct
  FROM customers;
```

**Root cause:** The second argument `'schema'` was passed as a literal string.
Spark tried to parse it as a DDL type name and failed with
`UNSUPPORTED_DATATYPE` because `SCHEMA` is not a valid data type.

**Fix:** Replaced `'schema'` with `schema_of_json(...)` using a sample JSON
value from the `profile` column to generate the correct DDL schema string:
```sql
SELECT from_json(profile, schema_of_json('{"first_name":"Dniren",...}')) AS profile_struct
  FROM customers;
```

---

## Notebook 2.4 — Higher Order Functions and SQL UDFs

### Cell 2 — Setup: Copy-Datasets to UC Volume

Same fix as Notebook 2.1 Cell 3: replaced `%run ../Includes/Copy-Datasets`
with inline Python that sets `dataset_bookstore` to the UC volume path.

No other cells required changes. All SQL cells reference UC tables (`orders`,
`customers`) or create SQL functions — both work on Serverless without modification.

---

## Notebook 2.5 — Data Transformations with PySpark

### Cell 1 — Setup: Copy-Datasets to UC Volume

Same fix as Notebook 2.1 Cell 3: replaced `%run ../Includes/Copy-Datasets`
with inline Python that sets `dataset_bookstore` to the UC volume path.

No other cells required changes. All Python cells use `spark.read.table()` and
`spark.table()` to load UC tables and perform DataFrame operations — all
compatible with Serverless Spark Connect.

---

## Summary of Serverless Limitations Encountered

| Limitation | Error Class | Affected Notebooks / Cells | Fix |
|---|---|---|---|
| Custom Spark config keys not settable | `CONFIG_NOT_AVAILABLE` | 2.1 Cell 3; 2.2 Cell 2; 2.3 Cell 2; 2.4 Cell 2; 2.5 Cell 1 | Inline Python variable + UC volume |
| `${param}` SQL substitution fails | `PATH_NOT_FOUND` | 2.1 Cells 5–8, 11, 13, 15, 27, 28, 29; 2.2 Cells 3, 6, 8, 10, 12, 15, 16; 2.3 Cell 23 | Literal volume path |
| `input_file_name()` blocked | `UC_COMMAND_NOT_SUPPORTED` | 2.1 Cell 9 | `_metadata.file_path` |
| `CREATE TABLE ... LOCATION` blocked | `Missing cloud file system scheme` / `dbfs not supported` | 2.1 Cell 16 | Temp view + CTAS to Delta |
| `REFRESH TABLE` blocked | `NOT_SUPPORTED_WITH_SERVERLESS` | 2.1 Cell 24 | Explanatory SELECT (Delta auto-refreshes) |
| DBFS root disabled | `DBFS_DISABLED` | 2.1 Cell 3 (original path) | UC volume |
| Managed non-Delta tables blocked | `MANAGED_TABLE_FORMAT` | 2.1 Cell 16 (alt approach) | CTAS with default Delta |
| Table already exists on re-run | `TABLE_OR_VIEW_ALREADY_EXISTS` | 2.2 Cell 3 | `CREATE OR REPLACE TABLE` |
| Invalid `from_json` schema argument | `UNSUPPORTED_DATATYPE` | 2.3 Cell 8 | `schema_of_json()` with sample JSON |