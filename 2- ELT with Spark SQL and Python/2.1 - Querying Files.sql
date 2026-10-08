-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- DBTITLE 1,Setup: Copy-Datasets to UC Volume
-- MAGIC %python
-- MAGIC data_source_uri = "s3://dalhussein-courses/datasets/bookstore/v1/"
-- MAGIC dataset_bookstore = '/Volumes/workspace/default/bookstore_data'
-- MAGIC data_catalog = 'workspace'
-- MAGIC
-- MAGIC # Copy dataset from S3 to UC volume (only needed once)
-- MAGIC from pyspark.sql.functions import col
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

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`/Volumes/workspace/default/bookstore_data/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`/Volumes/workspace/default/bookstore_data/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`/Volumes/workspace/default/bookstore_data/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`/Volumes/workspace/default/bookstore_data/customers-json`

-- COMMAND ----------

 SELECT *,
    _metadata.file_path source_file
  FROM json.`/Volumes/workspace/default/bookstore_data/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

SELECT * FROM text.`/Volumes/workspace/default/bookstore_data/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/workspace/default/bookstore_data/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

SELECT * FROM csv.`/Volumes/workspace/default/bookstore_data/books-csv`

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_csv_tmp
USING CSV
OPTIONS (
  path = "/Volumes/workspace/default/bookstore_data/books-csv",
  header = "true",
  delimiter = ";"
);

CREATE OR REPLACE TABLE books_csv AS
SELECT
  book_id,
  title,
  author,
  category,
  CAST(price AS DOUBLE) AS price
FROM books_csv_tmp

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- REFRESH TABLE is not supported on serverless compute.
-- Managed Delta tables maintain metadata automatically; no refresh needed.
SELECT 'REFRESH TABLE not needed for managed Delta tables on serverless' AS note

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`/Volumes/workspace/default/bookstore_data/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`/Volumes/workspace/default/bookstore_data/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "/Volumes/workspace/default/bookstore_data/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books