-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 1,Setup: Copy-Datasets to UC Volume
-- MAGIC %python
-- MAGIC data_source_uri = "s3://dalhussein-courses/datasets/bookstore/v1/"
-- MAGIC dataset_bookstore = '/Volumes/workspace/default/bookstore_data'
-- MAGIC data_catalog = 'workspace'
-- MAGIC
-- MAGIC # Copy dataset from S3 to UC volume (only needed once)
-- MAGIC if len(dbutils.fs.ls(dataset_bookstore)) == 0:
-- MAGIC     print("Copying bookstore dataset from S3 to UC volume...")
-- MAGIC     dbutils.fs.cp(data_source_uri, f"{dataset_bookstore}/", True)
-- MAGIC     print("Copy complete!")
-- MAGIC else:
-- MAGIC     print(f"Dataset already available at {dataset_bookstore}")
-- MAGIC
-- MAGIC # spark.conf.set for custom keys is not available on Serverless Spark Connect
-- MAGIC # SQL cells use the volume path directly instead of ${dataset.bookstore}

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`/Volumes/workspace/default/bookstore_data/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`/Volumes/workspace/default/bookstore_data/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`/Volumes/workspace/default/bookstore_data/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`/Volumes/workspace/default/bookstore_data/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`/Volumes/workspace/default/bookstore_data/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`/Volumes/workspace/default/bookstore_data/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "/Volumes/workspace/default/bookstore_data/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Try cell below twice, if we try to rerun this statement, it will not reinsert those records as they are already on the table

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *