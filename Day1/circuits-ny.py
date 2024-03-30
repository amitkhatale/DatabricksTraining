# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw/

# COMMAND ----------

df=spark.read.csv('dbfs:/FileStore/tables/formula1_raw/circuits.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Select

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select('*').display()

# COMMAND ----------

df.select('circuitId','circuitRef').display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id")).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(concat("location"," ","country")).display()

# COMMAND ----------

df.select(concat("location",lit(" "),"country").alias("loc&country")).display()

# COMMAND ----------

df.withColumnRenamed('circuitId','circuit_id').withColumnRenamed('circuitRef','circuit_Ref').display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_column_names=['circuitId',
 'circuitRef',
 'name',
 'location',
 'country',
 'latitude',
 'longitude',
 'altitude',
 'url']

# COMMAND ----------

df.toDF(new_column_names)

# COMMAND ----------

df1=df.toDF(*new_column_names)

# COMMAND ----------

df1.display()

# COMMAND ----------

df.drop('url').display()

# COMMAND ----------

df.display()

# COMMAND ----------

df1.withColumn("current_time",current_date()).display()

# COMMAND ----------

df1.withColumn("upper_name",upper(col("name"))).display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/output/Amit/

# COMMAND ----------

df.write.parquet('dbfs:/FileStore/tables/output/Amit/circuit')

# COMMAND ----------

df=spark.read.parquet('dbfs:/FileStore/tables/output/Amit/circuit').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists amit

# COMMAND ----------

df1.write.saveAsTable('amit.circuit')

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from amit.circuit

# COMMAND ----------


