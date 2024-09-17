# Databricks notebook source
# MAGIC %md 
# MAGIC # Using Files in Databricks
# MAGIC ***

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Notebook Parameters
# MAGIC ***

# COMMAND ----------

dbutils.widgets.text("bundle.catalog", "", "catalog")

# COMMAND ----------

catalog_use = dbutils.widgets.get("bundle.catalog")
catalog_use

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Set Catalog and Schema To Use
# MAGIC ***

# COMMAND ----------

spark.sql(f"use catalog {catalog_use}")
spark.sql("use schema synthea")
display(spark.sql("select current_catalog(), current_schema()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listing Files in a Volume
# MAGIC ***

# COMMAND ----------

dbutils.fs.ls("/Volumes/resources/synthea/landing")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC list "/Volumes/resources/synthea/landing/"

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC list "/Volumes/resources/synthea/landing/allergies/"

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC ls -altR /Volumes/resources/synthea/landing/

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Copying Files Between Volumes 
# MAGIC ***

# COMMAND ----------

import subprocess

# COMMAND ----------

cmd = f"cp -r /Volumes/resources/synthea/landing/* /Volumes/{catalog_use}/synthea/landing/"
result = subprocess.run(cmd, shell=True, capture_output=True)
print(
    result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
)

# COMMAND ----------

cmd = f"ls -altR /Volumes/{catalog_use}/synthea/landing/"
result = subprocess.run(cmd, shell=True, capture_output=True)
print(
    result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: You Can Also Access Workspace Files
# MAGIC ***

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC pwd;

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls -alt ../fixtures/images

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Files Contents
# MAGIC *** 

# COMMAND ----------

dbutils.fs.head(f"/Volumes/{catalog_use}/synthea/landing/patients/2024_09_14T19_14_42Z_patients.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query an Individual File
# MAGIC ***

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("escape", '"') \
    .load(f"/Volumes/{catalog_use}/synthea/landing/patients/2024_09_14T19_14_42Z_patients.csv")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files(
# MAGIC   "/Volumes/mgiglia/synthea/landing/patients/2024_09_14T19_14_42Z_patients.csv"
# MAGIC   ,format => 'csv'
# MAGIC   ,delimiter => ','
# MAGIC   ,header => true
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
