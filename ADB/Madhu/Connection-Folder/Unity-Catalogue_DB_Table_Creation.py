# Databricks notebook source
# MAGIC %sql
# MAGIC create table IF NOT EXISTS cleansed_tables
# MAGIC USING DELTA

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/cleansed_datalake1/plane/")
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("catalogue.schema.cleansed_tables")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_tables

# COMMAND ----------

def pre_schema(folder, tablename):
    try:
        df = spark.read.format("delta").load(f"/mnt/cleansed_datalake1/{folder}/")
        spark.sql(f""" 
                  CREATE TABLE IF NOT EXISTS {tablename} USING DELTA
                  """)
        df.write.format("delta").mode("overwrite").saveAsTable(f"catalogue.schema.{tablename}")
    except Exception as err:
        print("Error Occurred", str(err))

# COMMAND ----------

pre_schema('plane','plane')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE catalogue.schema.cleansed_tables
