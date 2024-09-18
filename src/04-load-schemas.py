# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

import dlt

# COMMAND ----------

# # used for active development, but not run during DLT execution, use DLT configurations instead
# dbutils.widgets.text(name = "catalog", defaultValue="", label="catalog")
# dbutils.widgets.text("bundle.sourcePath", ".", "bundle.sourcePath")
# dbutils.widgets.text("bundle.fixturePath", "../fixtures", "bundle.fixturePath")

# spark.conf.set("bundle.catalog", dbutils.widgets.get(name = "catalog"))
# spark.conf.set("bundle.sourcePath", dbutils.widgets.get(name = "bundle.sourcePath"))
# spark.conf.set("bundle.fixturePath", dbutils.widgets.get(name = "bundle.fixturePath"))

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

ddl_path = os.path.abspath(spark.conf.get('bundle.fixturePath') + "/ddl")
ddl_path

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

ddl_files = main.retrieve_ddl_files(
  ddl_path = ddl_path
  ,workspace_client = w
) 

# COMMAND ----------

catalog_use = spark.conf.get("bundle.catalog")
volume_path = f"/Volumes/resources/synthea/landing"
print(f"""
    volume_path = {volume_path}
""")

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = volume_path
)

# COMMAND ----------

Pipeline.load_ddl_files(
  ddl_df = ddl_files
)
