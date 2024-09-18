# Databricks notebook source
import dlt

# COMMAND ----------

# # used for active development, but not run during DLT execution, use DLT configurations instead
# dbutils.widgets.text(name = "catalog", defaultValue="", label="catalog")
# dbutils.widgets.text("bundle.sourcePath", ".", "bundle.sourcePath")
# dbutils.widgets.text("bundle.fixturePath", "../fixtures", "bundle.fixturePath")
# dbutils.widgets.text("bundle.volumePath", "../fixtures", "bundle.volumePath")

# spark.conf.set("bundle.catalog", dbutils.widgets.get(name = "catalog"))
# spark.conf.set("bundle.sourcePath", dbutils.widgets.get(name = "bundle.sourcePath"))
# spark.conf.set("bundle.fixturePath", dbutils.widgets.get(name = "bundle.fixturePath"))
# spark.conf.set("bundle.volumePath", dbutils.widgets.get(name = "bundle.volumePath"))

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

catalog_use = spark.conf.get("bundle.catalog")
volume_path = f"/Volumes/resources/synthea/landing"
print(f"""
    volume_path = {volume_path}
""")

# COMMAND ----------

key_path = os.path.abspath(spark.conf.get('bundle.fixturePath') + "/keys")
sys.path.append(os.path.abspath(key_path))

import keys

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = volume_path
)

# COMMAND ----------

ddl_ref = spark.sql(f"select table_name, ddl from {catalog_use}.synthea.silver_schemas").collect()
ddl_ref = [row.asDict() for row in ddl_ref]
ddl_ref

# COMMAND ----------

for i in ddl_ref:
  table_name = i["table_name"]
  ddl = i["ddl"]
  Pipeline.stage_silver(
    bronze_table = f"{catalog_use}.synthea.{table_name}_bronze"
    ,table_name = table_name
    ,ddl = ddl
  )

# COMMAND ----------

for i in ddl_ref:
  table_name = i["table_name"]
  expect_all_or_drop = None
  expect_all = None
  if table_name == "encounters": 
    expect_all_or_drop = {"valid patient_id ": "patient_id IS NOT NULL"}
    expect_all = {'positive payer_coverage': "payer_coverage >= 0"}
  elif table_name == "claims_transactions":
    expect_all = {'positive payments': "payments >= 0"}
  elif table_name == "medications": 
    expect_all = {"postive total_cost": "total_cost >= 0"}
  Pipeline.stream_silver(
    bronze_table = f"{catalog_use}.synthea.{table_name}_bronze"
    ,table_name = table_name
    ,sequence_by = "sequence_by"
    ,keys = getattr(keys, table_name)
    ,expect_all_or_drop = expect_all_or_drop
    ,expect_all = expect_all
  )
