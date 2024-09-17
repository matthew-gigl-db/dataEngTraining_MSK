# Databricks notebook source
import dlt

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

key_path = os.path.abspath(spark.conf.get('bundle.fixturePath') + "/keys")
sys.path.append(os.path.abspath(key_path))

import keys

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = volume_path
)

# COMMAND ----------

ddl_ref = spark.sql(f"select table_name, ddl from {catalog_use}.synthea.synthea_silver_schemas").collect()
ddl_ref = [row.asDict() for row in ddl_ref]
ddl_ref

# COMMAND ----------

for i in ddl_ref:
  table_name = i["table_name"]
  ddl = i["ddl"]
  expect_all_or_drop = None
  if table_name == "encounters": 
    expect_all_or_drop = {"valid patient_id ": "patient_id IS NOT NULL"}
  Pipeline.stage_silver(
    bronze_table = f"{catalog}.synthea.{table_name}_bronze"
    ,table_name = table_name
    ,ddl = ddl
    ,expect_all_or_drop = expect_all_or_drop
  )
