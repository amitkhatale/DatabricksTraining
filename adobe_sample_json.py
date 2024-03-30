# Databricks notebook source
# MAGIC %run /Workspace/Users/amit.khatale@mmc.com/Day2/includes

# COMMAND ----------

input_raw_files

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw_json/

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/adobe_sample_json.json",multiLine=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumn("topping",explode("topping")).display()

# COMMAND ----------

df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")\
.display()

# COMMAND ----------

df_final=df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")\
.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")

# COMMAND ----------

df_final.write.saveAsTable("amit.adobe_sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from amit.adobe_sample where topping_id=5001

# COMMAND ----------

df=spark.read.table("amit.adobe_sample")

# COMMAND ----------


