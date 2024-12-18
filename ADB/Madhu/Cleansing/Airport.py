# Databricks notebook source
dbutils.fs.rm("/dbfs/FileStore/tables/schema/Airport", True)
dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/Airport", True)

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/Airport")
    .load("/mnt/raw_datalake/Airport/")
)
#display(df.limit(2))

# COMMAND ----------

dbutils.fs.ls("/mnt/cleansed_datalake1")
#dbutils.fs.rm("/mnt/cleansed_datalake1", True)
#dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/PLANE", True)

# COMMAND ----------

df_base = df.selectExpr(
    "Code as code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
#display(df_base.limit(2))

# COMMAND ----------

df_base.writeStream.trigger(once=True).format("delta").option(
    "checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/Airport"
).start("/mnt/cleansed_datalake1/airport")

# COMMAND ----------

dbutils.fs.rm("/mnt/cleansed_datalake1/plane", recurse=True)

# COMMAND ----------

dbutils.fs.ls("/mnt/cleansed_datalake1/")

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
