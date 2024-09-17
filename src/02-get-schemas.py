# Databricks notebook source
dbutils.widgets.text("bundle.catalog", "", "catalog")

# COMMAND ----------

catalog_use = dbutils.widgets.get("bundle.catalog")
catalog_use

# COMMAND ----------

spark.sql(f"use catalog {catalog_use}")
spark.sql("use schema synthea")
display(spark.sql("select current_catalog(), current_schema()")) 

# COMMAND ----------

spark.sql(f"create volume if not exists fixtures")

# COMMAND ----------

import subprocess

cmd = f"cp -R ../fixtures/* /Volumes/{catalog_use}/synthea/fixtures/"
result = subprocess.run(cmd, shell=True, capture_output=True)
file_list = result.stdout.decode('utf-8').split('\n')

# COMMAND ----------

ddl_list = dbutils.fs.ls(f"/Volumes/{catalog_use}/synthea/fixtures/ddl/")
display(ddl_list)

# COMMAND ----------

ddl_names = [item.name.replace('.ddl', '') for item in ddl_list]
ddl_names

# COMMAND ----------

def get_schema_struct(directory_name, volume_path): 
    full_path = volume_path + directory_name
    stmnt = f"""
        select * from read_files(
            '{full_path}'
            ,format => 'csv'
            ,delimiter => ','
            ,header => true
        )
    """
    df = spark.sql(stmnt)
    return str(df.schema)

# COMMAND ----------

get_schema_struct(directory_name = ddl_names[0], volume_path = f"/Volumes/{catalog_use}/synthea/landing/")

# COMMAND ----------

schema_dict = {filename: get_schema_struct(directory_name=filename, volume_path=f"/Volumes/{catalog_use}/synthea/landing/") for filename in file_list}

# COMMAND ----------

print(schema_dict)
