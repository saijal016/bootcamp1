# Databricks notebook source
df=spark.read.json("dbfs:/mnt/saunextadls/raw/json/")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

 df1=df.withColumn("ingestiondate",current_timestamp()).withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").option("path","dbfs:/mnt/saunextadls/raw/output/saijal/json").saveAsTable("json.bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from json.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.bronze

# COMMAND ----------


