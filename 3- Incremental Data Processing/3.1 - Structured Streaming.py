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

# spark.conf.set for custom keys is not available on Serverless Spark Connect
# SQL cells use the volume path directly instead of ${dataset.bookstore}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

stream = (spark.table("books_streaming_tmp_vw")
          .writeStream
          .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/books_streaming_display")
          .outputMode("append")
          .trigger(availableNow=True)
          .table("books_streaming_display"))
stream.awaitTermination()
display(spark.table("books_streaming_display"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

from pyspark.sql.functions import count

stream = (spark.table("books_streaming_tmp_vw")
          .groupBy("author")
          .agg(count("book_id").alias("total_books"))
          .writeStream
          .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/books_streaming_agg")
          .outputMode("complete")
          .trigger(availableNow=True)
          .table("books_streaming_agg"))
stream.awaitTermination()
display(spark.table("books_streaming_agg"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# Note: ORDER BY is not supported in streaming queries without a watermark.
# This cell demonstrates that limitation. On Serverless, a checkpoint location
# is also required to attempt the query.
stream = (spark.table("books_streaming_tmp_vw")
          .orderBy("author")
          .writeStream
          .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/books_streaming_orderby")
          .outputMode("append")
          .trigger(availableNow=True)
          .table("books_streaming_orderby"))
stream.awaitTermination()
display(spark.table("books_streaming_orderby"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

from pyspark.sql.functions import count

(spark.table("books_streaming_tmp_vw")
      .groupBy("author")
      .agg(count("book_id").alias("total_books"))
      .createOrReplaceTempView("author_counts_tmp_vw"))

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/author_counts")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "/Volumes/workspace/default/bookstore_data/checkpoints/author_counts")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts