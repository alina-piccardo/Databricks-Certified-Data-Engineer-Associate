# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Setup: Copy-Datasets to UC Volume
data_source_uri = "s3://dalhussein-courses/datasets/bookstore/v1/"
dataset_bookstore = '/Volumes/workspace/default/bookstore_data'
data_catalog = 'workspace'

# Copy dataset from S3 to UC volume (only needed once)
if len(dbutils.fs.ls(dataset_bookstore)) == 0:
    print("Copying bookstore dataset from S3 to UC volume...")
    dbutils.fs.cp(data_source_uri, f"{dataset_bookstore}/", True)
    print("Copy complete!")
else:
    print(f"Dataset already available at {dataset_bookstore}")

# Helper functions for streaming data simulation (replaces Includes/Copy-Datasets)
def get_index(dir):
    files = dbutils.fs.ls(dir)
    index = 0
    if files:
        file = max(files).name
        index = int(file.rsplit('.', maxsplit=1)[0])
    return index+1

streaming_dir = f"{dataset_bookstore}/orders-streaming"
raw_dir = f"{dataset_bookstore}/orders-raw"

def load_file(current_index):
    latest_file = f"{str(current_index).zfill(2)}.parquet"
    print(f"Loading {latest_file} file to the bookstore dataset")
    dbutils.fs.cp(f"{streaming_dir}/{latest_file}", f"{raw_dir}/{latest_file}")

def load_new_data(all=False):
    index = get_index(raw_dir)
    if index >= 10:
        print("No more data to load\n")
    elif all == True:
        while index <= 10:
            load_file(index)
            index += 1
    else:
        load_file(index)
        index += 1

# spark.conf.set for custom keys is not available on Serverless Spark Connect
# SQL cells use the volume path directly instead of ${dataset.bookstore}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enriching Raw Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() arrival_time
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

stream = (spark.table("orders_tmp")
          .writeStream
          .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/orders_tmp_display")
          .outputMode("append")
          .trigger(availableNow=True)
          .table("orders_tmp_display"))
stream.awaitTermination()
display(spark.table("orders_tmp_display"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/orders_bronze")
      .outputMode("append")
      .trigger(availableNow=True)
      .table("orders_bronze")
      .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating Static Lookup Table

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/orders_silver")
      .outputMode("append")
      .trigger(availableNow=True)
      .table("orders_silver")
      .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()