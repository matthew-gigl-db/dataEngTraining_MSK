# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("bundle.catalog", "", "catalog")

# COMMAND ----------

catalog_use = dbutils.widgets.get("bundle.catalog")
catalog_use

# COMMAND ----------

spark.sql(f"use catalog {catalog_use}")
spark.sql("use schema synthea")
display(spark.sql("select current_catalog(), current_schema()"))

# COMMAND ----------

import openai
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
# DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
# Alternatively in a Databricks notebook you can use this:
# DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

openai.api_key = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
openai.api_base = "https://dbc-5ca1a49e-7215.cloud.databricks.com/serving-endpoints"

def get_schema_from_llm(prompt):
  chat_completion = openai.ChatCompletion.create(
    messages=[
    {
      "role": "system",
      "content": "You are a Databricks AI assistant that is capable of reviewing the schema of a datafame and returning the syntatically correct pyspark struct version of that dataframe's schema. If the schema contains a column simply called 'id' then it should be renamed to match the name of the subdirectory the files are in.  For example, if the files were loaded from .../patients then the 'id' column should be name 'patient_id' instead. Don't include any commentary about the answer, just the perfect pyspark itself. "
    },
    {
      "role": "user",
      "content": prompt
    }
    ],
    model="databricks-dbrx-instruct",
    max_tokens=4096
  )
  return chat_completion.choices[0].message.content

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from read_files(
# MAGIC   "/Volumes/mgiglia/synthea/landing/careplans/"
# MAGIC   ,format => 'csv'
# MAGIC   ,delimiter => ','
# MAGIC   ,header => true
# MAGIC   -- ,schemaHints => 'FIPS string, ZIP string'
# MAGIC )

# COMMAND ----------

print(get_schema_from_llm(prompt = f"write a perfect struct schema for this df: {_sqldf}"))
