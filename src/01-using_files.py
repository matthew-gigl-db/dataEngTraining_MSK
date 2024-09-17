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

# MAGIC %sql
# MAGIC describe select * from read_files(
# MAGIC   "/Volumes/mgiglia/synthea/landing/patients/2024_09_14T19_14_42Z_patients.csv"
# MAGIC   ,format => 'csv'
# MAGIC   ,delimiter => ','
# MAGIC   ,header => true
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files(
# MAGIC   "/Volumes/mgiglia/synthea/landing/patients/2024_09_14T19_14_42Z_patients.csv"
# MAGIC   ,format => 'csv'
# MAGIC   ,delimiter => ','
# MAGIC   ,header => true
# MAGIC   ,schemaHints => 'FIPS string, ZIP string'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use the Databricks Assistant to Get the Struct Version of the Schema for pySpark
# MAGIC ***
# MAGIC
# MAGIC Note that the last dataframe value from our SQL statement is stored as _sqldf.  
# MAGIC
# MAGIC Type "command i" to open the Databricks Assistant and type:  "return the struct schema of _sqldf"

# COMMAND ----------


from pyspark.sql.types import *

patients_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("BIRTHDATE", DateType(), True),
    StructField("DEATHDATE", DateType(), True),
    StructField("SSN", StringType(), True),
    StructField("DRIVERS", StringType(), True),
    StructField("PASSPORT", StringType(), True),
    StructField("PREFIX", StringType(), True),
    StructField("FIRST", StringType(), True),
    StructField("MIDDLE", StringType(), True),
    StructField("LAST", StringType(), True),
    StructField("SUFFIX", StringType(), True),
    StructField("MAIDEN", StringType(), True),
    StructField("MARITAL", StringType(), True),
    StructField("RACE", StringType(), True),
    StructField("ETHNICITY", StringType(), True),
    StructField("GENDER", StringType(), True),
    StructField("BIRTHPLACE", StringType(), True),
    StructField("ADDRESS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("COUNTY", StringType(), True),
    StructField("FIPS", IntegerType(), True),
    StructField("ZIP", IntegerType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("HEALTHCARE_EXPENSES", DoubleType(), True),
    StructField("HEALTHCARE_COVERAGE", DoubleType(), True),
    StructField("INCOME", IntegerType(), True),
    StructField("_rescued_data", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query All of the Files In the Directory With the Proper Schema
# MAGIC ***

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("escape", '"') \
    .schema(patients_schema) \
    .load(f"/Volumes/{catalog_use}/synthea/landing/patients/")

display(df)
