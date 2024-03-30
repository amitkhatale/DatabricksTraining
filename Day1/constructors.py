# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw/

# COMMAND ----------

df=spark.read.json('dbfs:/FileStore/tables/formula1_raw/constructors.json')

# COMMAND ----------

df.drop(col('url'))
