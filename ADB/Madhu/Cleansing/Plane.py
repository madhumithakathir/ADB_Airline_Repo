# Databricks notebook source
df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/PLANE")
    .load("/mnt/raw_datalake/Plane/")
)
#display(df)

# COMMAND ----------

#dbutils.fs.ls("/mnt/cleansed_datalake1")
#dbutils.fs.rm("/mnt/cleansed_datalake1", True)
dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/PLANE", True)
dbutils.fs.rm("/dbfs/FileStore/tables/schema/PLANE", True)

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/cleansed_datalake1/plane")
display(df.limit(2))

# COMMAND ----------

df_base = df.selectExpr(
    "tailnum as tailid",
    "type",
    "manufacturer",
    "to_date(issue_date) as issue_date",
    "model",
    "status",
    "aircraft_type",
    "engine_type",
    "cast(year as int) as year",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part",
)


# COMMAND ----------

df_base.writeStream.trigger(once=True).format("delta").option(
    "checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/PLANE"
).start("/mnt/cleansed_datalake1/plane")

# COMMAND ----------

dbutils.fs.rm("/mnt/cleansed_datalake1/plane", recurse=True)

# COMMAND ----------

def pre_schema(df):
    try:
        schema=""
        for i in df.dtypes:
            schema=schema+i[0]+" "+i[1]+","
        return schema[:-1]
    except:
        return "Error"

# COMMAND ----------

def f_delta_cleansed_load1(table_name, location, schema, database):
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {database}.{table_name} ({schema}) USING DELTA LOCATION '{location}'")
    except Exception as err:
        print("Error",str(err))

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/cleansed_datalake1/plane/")
schema = pre_schema(df)
f_delta_cleansed_load1('cleansed_tables', 'abfss://cleansed@uatadlsraw.dfs.core.windows.net/', schema, 'schema')

#abfss://cleansed@uatadlsraw.dfs.core.windows.net/
