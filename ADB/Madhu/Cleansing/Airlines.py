# Databricks notebook source
from pyspark.sql.functions import explode

df = spark.read.json("/mnt/raw_datalake/Airlines/")
df1 = df.select(explode("response"), "Date_Part")
df_final = df1.select("col.*", "Date_Part")
display(df_final)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

# MAGIC %run /Madhu/Connection-Folder/Table_Creation

# COMMAND ----------

pre_schema('airline')

# COMMAND ----------

pre_schema('airline')
