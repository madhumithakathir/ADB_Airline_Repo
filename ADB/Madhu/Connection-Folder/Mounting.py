# Databricks notebook source
dbutils.fs.ls("/mnt/source_blob/")

# COMMAND ----------

# Retrieve secrets
container_name = dbutils.secrets.get(scope="uatsecretscope", key="containername1")
storage_account_name = dbutils.secrets.get(scope="uatsecretscope", key="storageaccountname2")
sas = dbutils.secrets.get(scope="uatsecretscope", key="sas1")
config = f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net"

# Mount the storage
dbutils.fs.mount(
    source=dbutils.secrets.get(scope="uatsecretscope", key="blob-mnt-path1"),
    mount_point="/mnt/source_blob/",
    extra_configs={config: sas}
)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "uatsecretscope", key = "SPid"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="uatsecretscope",key="SPsecret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "uatsecretscope", key = "data-client-refresh-url")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
mountPoint="/mnt/raw_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = dbutils.secrets.get(scope = "uatsecretscope", key = "datalake-raw"),
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "uatsecretscope", key = "SPid"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="uatsecretscope",key="SPsecret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "uatsecretscope", key = "data-client-refresh-url")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
mountPoint="/mnt/cleansed_datalake/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = dbutils.secrets.get(scope = "uatsecretscope", key = "datalake-cleansed1"),
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

# Install necessary libraries
%pip install tabula-py pandas

import os
import tabula
from datetime import date
import pandas as pd

source = '/dbfs/mnt/source_blob/PLANE.pdf'
destination_dir = f'/dbfs/mnt/raw_datalake/Plane/Date_Part={date.today()}'
destination = f'{destination_dir}/Plane.csv'

# Create the destination directory if it does not exist
os.makedirs(destination_dir, exist_ok=True)

# Read the PDF into a list of DataFrames
dfs = tabula.read_pdf(source, pages='all')

# Concatenate all DataFrames into a single DataFrame
df = pd.concat(dfs, ignore_index=True)

# Save the DataFrame as a CSV
df.to_csv(destination, index=False)

display(df)
