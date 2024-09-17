# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from typing import Callable
import os
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import *

# COMMAND ----------

# used for active development, but not run during DLT execution, use DLT configurations instead
# dbutils.widgets.text(name = "catalog", defaultValue="", label="catalog")
# dbutils.widgets.text("bundle.sourcePath", ".", "bundle.sourcePath")

# spark.conf.set("bundle.catalog", dbutils.widgets.get(name = "catalog"))
# spark.conf.set("bundle.sourcePath", dbutils.widgets.get(name = "bundle.sourcePath"))

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

catalog_use = spark.conf.get("bundle.catalog")
volume_path = f"/Volumes/{catalog_use}/synthea/landing"
print(f"""
    volume_path = {volume_path}
""")

# COMMAND ----------

# List directories in the volume path
dir_list = dbutils.fs.ls(volume_path)

# Extract directory names and create a list
dir_names = [item.name.replace("/", "") for item in dir_list if item.isDir()]
dir_names

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = volume_path
)

# COMMAND ----------

Pipeline

# COMMAND ----------

# Pipeline.ingest_raw_to_bronze(
#     table_name=f"{dir_names[0]}_bronze"
#     ,table_comment=f"A full text record of files from {volume_path}/{dir_names[0]}."
#     ,table_properties={"quality":"bronze", "phi":"True", "pii":"True", "pci":"False"}
#     ,source_folder_path_from_volume=f"{dir_names[0]}"
#     ,file_type = "csv"
# )

# COMMAND ----------

for dir_name in dir_names:
    Pipeline.ingest_raw_to_bronze(
        table_name=f"{dir_name}_bronze",
        table_comment=f"A full text record of files from {volume_path}/{dir_name}.",
        table_properties={"quality":"bronze", "phi":"True", "pii":"True", "pci":"False"},
        source_folder_path_from_volume=f"{dir_name}",
        file_type="csv"
    )
