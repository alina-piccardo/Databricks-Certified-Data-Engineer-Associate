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

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can see that the Books column is of complex data type (array of a struct type), to work directly with such a complex datatype, we need to use Higher Order Functions, as they allow you to work directly with hierarchical data like arrays and map type objects

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtering Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transforming Arrays

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;