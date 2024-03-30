Reexecute the change-- to check Git
# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw

# COMMAND ----------

from pyspark.sql.functions import *
df_final=df.withColumn("ingestion_date",current_timestamp()).drop("url")

# COMMAND ----------

df_final.write.saveAsTable("amit.constructor2")

# COMMAND ----------

spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json").withColumn("ingestion_date",current_timestamp()).drop("url").write.mode("overwrite").saveAsTable("amit.constructor2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json. `dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table amit.constructor3 as
# MAGIC select *,current_timestamp() as ingestion_date from json.`dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amit.constructor3

# COMMAND ----------


