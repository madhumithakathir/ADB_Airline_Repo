# Databricks notebook source
def pre_schema(tablename):
    try:
        df = spark.read.format("delta").load(f"/mnt/cleansed_datalake1/{tablename}/")
        spark.sql(f""" 
                  CREATE TABLE IF NOT EXISTS {tablename} USING DELTA
                  """)
        df.write.format("delta").mode("overwrite").saveAsTable(f"catalogue.schema.{tablename}")
    except Exception as err:
        print("Error Occurred", str(err))

# COMMAND ----------

pre_schema('cancellation')
