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
# MAGIC ## Exploring The Source Directory

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
        .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/orders")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/orders")
        .table("orders_updates")
        .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Landing New Files

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("/Volumes/workspace/default/bookstore_data/checkpoints/orders", True)