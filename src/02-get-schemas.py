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

import subprocess

cmd = f"ls /Volumes/{catalog_use}/synthea/landing"
result = subprocess.run(cmd, shell=True, capture_output=True)
file_list = result.stdout.decode('utf-8').split('\n')

# COMMAND ----------

file_list = [file for file in file_list if file != '']
file_list

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

get_schema_struct(directory_name = file_list[0], volume_path = f"/Volumes/{catalog_use}/synthea/landing/")

# COMMAND ----------

schema_dict = {filename: get_schema_struct(directory_name=filename, volume_path=f"/Volumes/{catalog_use}/synthea/landing/") for filename in file_list}

# COMMAND ----------

print(schema_dict)

# COMMAND ----------

spark.sql(f"create volume if not exists ddl")

# COMMAND ----------

with open(f"/Volumes/{catalog_use}/synthea/ddl/schema_dict.json", "w") as file:
    file.write(str(schema_dict))

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
      "content": "You are a Databricks expert reviewing a dictionary of schemas with a filename and a schema.  Fix the dictionary such that if the schema contains a column called 'id' that the column be renamed based on the singular version of the filenames. For example, if the filename is 'patients' and the schema contains a column named 'id' then the column should be renamed to 'patient_id'.  After you fix any 'id' columns to their proper names, check over all the schemas.  This is a data model, change and forieng keys you find to the primary key name that you fixed in the previous step.  For example, if 'patient' is a foriegn key in the schema, then change it to 'patient_id'.  The schema should be returned as an updated version of the dictionary. Don't give an explanation for the answer, just provide the dictionary containing the schema. "
    },
    {
      "role": "user",
      "content": prompt
    }
    ],
    model="databricks-meta-llama-3-1-70b-instruct",
    max_tokens=8192
  )
  return chat_completion.choices[0].message.content

# COMMAND ----------

# result = (get_schema_from_llm(str(schema_dict)))

# COMMAND ----------

print(result)

# COMMAND ----------

schemas = {
    'allergies': "StructType([StructField('START', DateType(), True), StructField('STOP', StringType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CODE', LongType(), True), StructField('SYSTEM', StringType(), True), StructField('DESCRIPTION', StringType(), True), StructField('TYPE', StringType(), True), StructField('CATEGORY', StringType(), True), StructField('REACTION1', IntegerType(), True), StructField('DESCRIPTION1', StringType(), True), StructField('SEVERITY1', StringType(), True), StructField('REACTION2', IntegerType(), True), StructField('DESCRIPTION2', StringType(), True), StructField('SEVERITY2', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'careplans': "StructType([StructField('careplan_id', StringType(), True), StructField('START', DateType(), True), StructField('STOP', DateType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CODE', IntegerType(), True), StructField('DESCRIPTION', StringType(), True), StructField('REASONCODE', LongType(), True), StructField('REASONDESCRIPTION', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'claims': "StructType([StructField('claim_id', StringType(), True), StructField('PATIENTID', StringType(), True), StructField('PROVIDERID', StringType(), True), StructField('PRIMARYPATIENTINSURANCEID', StringType(), True), StructField('SECONDARYPATIENTINSURANCEID', StringType(), True), StructField('DEPARTMENTID', IntegerType(), True), StructField('PATIENTDEPARTMENTID', IntegerType(), True), StructField('DIAGNOSIS1', LongType(), True), StructField('DIAGNOSIS2', LongType(), True), StructField('DIAGNOSIS3', LongType(), True), StructField('DIAGNOSIS4', LongType(), True), StructField('DIAGNOSIS5', LongType(), True), StructField('DIAGNOSIS6', LongType(), True), StructField('DIAGNOSIS7', LongType(), True), StructField('DIAGNOSIS8', IntegerType(), True), StructField('REFERRINGPROVIDERID', StringType(), True), StructField('APPOINTMENTID', StringType(), True), StructField('CURRENTILLNESSDATE', TimestampType(), True), StructField('SERVICEDATE', TimestampType(), True), StructField('SUPERVISINGPROVIDERID', StringType(), True), StructField('STATUS1', StringType(), True), StructField('STATUS2', StringType(), True), StructField('STATUSP', StringType(), True), StructField('OUTSTANDING1', DoubleType(), True), StructField('OUTSTANDING2', DoubleType(), True), StructField('OUTSTANDINGP', DoubleType(), True), StructField('LASTBILLEDDATE1', TimestampType(), True), StructField('LASTBILLEDDATE2', TimestampType(), True), StructField('LASTBILLEDDATEP', TimestampType(), True), StructField('HEALTHCARECLAIMTYPEID1', IntegerType(), True), StructField('HEALTHCARECLAIMTYPEID2', IntegerType(), True), StructField('_rescued_data', StringType(), True)])",
    'claims_transactions': "StructType([StructField('claims_transaction_id', StringType(), True), StructField('CLAIMID', StringType(), True), StructField('CHARGEID', IntegerType(), True), StructField('PATIENTID', StringType(), True), StructField('TYPE', StringType(), True), StructField('AMOUNT', DoubleType(), True), StructField('METHOD', StringType(), True), StructField('FROMDATE', TimestampType(), True), StructField('TODATE', TimestampType(), True), StructField('PLACEOFSERVICE', StringType(), True), StructField('PROCEDURECODE', LongType(), True), StructField('MODIFIER1', StringType(), True), StructField('MODIFIER2', StringType(), True), StructField('DIAGNOSISREF1', IntegerType(), True), StructField('DIAGNOSISREF2', IntegerType(), True), StructField('DIAGNOSISREF3', IntegerType(), True), StructField('DIAGNOSISREF4', IntegerType(), True), StructField('UNITS', IntegerType(), True), StructField('DEPARTMENTID', IntegerType(), True), StructField('NOTES', StringType(), True), StructField('UNITAMOUNT', DoubleType(), True), StructField('TRANSFEROUTID', IntegerType(), True), StructField('TRANSFERTYPE', StringType(), True), StructField('PAYMENTS', DoubleType(), True), StructField('ADJUSTMENTS', DoubleType(), True), StructField('TRANSFERS', DoubleType(), True), StructField('OUTSTANDING', DoubleType(), True), StructField('APPOINTMENTID', StringType(), True), StructField('LINENOTE', StringType(), True), StructField('PATIENTINSURANCEID', StringType(), True), StructField('FEESCHEDULEID', IntegerType(), True), StructField('PROVIDERID', StringType(), True), StructField('SUPERVISINGPROVIDERID', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'conditions': "StructType([StructField('START', DateType(), True), StructField('STOP', DateType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('SYSTEM', StringType(), True), StructField('CODE', LongType(), True), StructField('DESCRIPTION', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'devices': "StructType([StructField('START', TimestampType(), True), StructField('STOP', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CODE', IntegerType(), True), StructField('DESCRIPTION', StringType(), True), StructField('UDI', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'encounters': "StructType([StructField('encounter_id', StringType(), True), StructField('START', TimestampType(), True), StructField('STOP', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('ORGANIZATION', StringType(), True), StructField('PROVIDER', StringType(), True), StructField('PAYER', StringType(), True), StructField('ENCOUNTERCLASS', StringType(), True), StructField('CODE', LongType(), True), StructField('DESCRIPTION', StringType(), True), StructField('BASE_ENCOUNTER_COST', DoubleType(), True), StructField('TOTAL_CLAIM_COST', DoubleType(), True), StructField('PAYER_COVERAGE', DoubleType(), True), StructField('REASONCODE', LongType(), True), StructField('REASONDESCRIPTION', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'imaging_studies': "StructType([StructField('imaging_study_id', StringType(), True), StructField('DATE', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('SERIES_UID', StringType(), True), StructField('BODYSITE_CODE', IntegerType(), True), StructField('BODYSITE_DESCRIPTION', StringType(), True), StructField('MODALITY_CODE', StringType(), True), StructField('MODALITY_DESCRIPTION', StringType(), True), StructField('INSTANCE_UID', StringType(), True), StructField('SOP_CODE', StringType(), True), StructField('SOP_DESCRIPTION', StringType(), True), StructField('PROCEDURE_CODE', LongType(), True), StructField('_rescued_data', StringType(), True)])",
    'immunizations': "StructType([StructField('DATE', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CODE', IntegerType(), True), StructField('DESCRIPTION', StringType(), True), StructField('BASE_COST', DoubleType(), True), StructField('_rescued_data', StringType(), True)])",
    'medications': "StructType([StructField('START', TimestampType(), True), StructField('STOP', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('PAYER', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CODE', IntegerType(), True), StructField('DESCRIPTION', StringType(), True), StructField('BASE_COST', DoubleType(), True), StructField('PAYER_COVERAGE', DoubleType(), True), StructField('DISPENSES', IntegerType(), True), StructField('TOTALCOST', DoubleType(), True), StructField('REASONCODE', LongType(), True), StructField('REASONDESCRIPTION', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'observations': "StructType([StructField('DATE', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CATEGORY', StringType(), True), StructField('CODE', StringType(), True), StructField('DESCRIPTION', StringType(), True), StructField('VALUE', StringType(), True), StructField('UNITS', StringType(), True), StructField('TYPE', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'organizations': "StructType([StructField('organization_id', StringType(), True), StructField('NAME', StringType(), True), StructField('ADDRESS', StringType(), True), StructField('CITY', StringType(), True), StructField('STATE', StringType(), True), StructField('ZIP', IntegerType(), True), StructField('LAT', DoubleType(), True), StructField('LON', DoubleType(), True), StructField('PHONE', StringType(), True), StructField('REVENUE', DoubleType(), True), StructField('UTILIZATION', IntegerType(), True), StructField('_rescued_data', StringType(), True)])",
    'patients': "StructType([StructField('patient_id', StringType(), True), StructField('BIRTHDATE', DateType(), True), StructField('DEATHDATE', DateType(), True), StructField('SSN', StringType(), True), StructField('DRIVERS', StringType(), True), StructField('PASSPORT', StringType(), True), StructField('PREFIX', StringType(), True), StructField('FIRST', StringType(), True), StructField('MIDDLE', StringType(), True), StructField('LAST', StringType(), True), StructField('SUFFIX', StringType(), True), StructField('MAIDEN', StringType(), True), StructField('MARITAL', StringType(), True), StructField('RACE', StringType(), True), StructField('ETHNICITY', StringType(), True), StructField('GENDER', StringType(), True), StructField('BIRTHPLACE', StringType(), True), StructField('ADDRESS', StringType(), True), StructField('CITY', StringType(), True), StructField('STATE', StringType(), True), StructField('COUNTY', StringType(), True), StructField('FIPS', IntegerType(), True), StructField('ZIP', IntegerType(), True), StructField('LAT', DoubleType(), True), StructField('LON', DoubleType(), True), StructField('HEALTHCARE_EXPENSES', DoubleType(), True), StructField('HEALTHCARE_COVERAGE', DoubleType(), True), StructField('INCOME', IntegerType(), True), StructField('_rescued_data', StringType(), True)])",
    'payer_transitions': "StructType([StructField('PATIENT', StringType(), True), StructField('MEMBERID', StringType(), True), StructField('START_DATE', TimestampType(), True), StructField('END_DATE', TimestampType(), True), StructField('PAYER', StringType(), True), StructField('SECONDARY_PAYER', StringType(), True), StructField('PLAN_OWNERSHIP', StringType(), True), StructField('OWNER_NAME', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'payers': "StructType([StructField('payer_id', StringType(), True), StructField('NAME', StringType(), True), StructField('OWNERSHIP', StringType(), True), StructField('ADDRESS', StringType(), True), StructField('CITY', StringType(), True), StructField('STATE_HEADQUARTERED', StringType(), True), StructField('ZIP', StringType(), True), StructField('PHONE', StringType(), True), StructField('AMOUNT_COVERED', DoubleType(), True), StructField('AMOUNT_UNCOVERED', DoubleType(), True), StructField('REVENUE', DoubleType(), True), StructField('COVERED_ENCOUNTERS', IntegerType(), True), StructField('UNCOVERED_ENCOUNTERS', IntegerType(), True), StructField('COVERED_MEDICATIONS', IntegerType(), True), StructField('UNCOVERED_MEDICATIONS', IntegerType(), True), StructField('COVERED_PROCEDURES', IntegerType(), True), StructField('UNCOVERED_PROCEDURES', IntegerType(), True), StructField('COVERED_IMMUNIZATIONS', IntegerType(), True), StructField('UNCOVERED_IMMUNIZATIONS', IntegerType(), True), StructField('UNIQUE_CUSTOMERS', IntegerType(), True), StructField('QOLS_AVG', DoubleType(), True), StructField('MEMBER_MONTHS', IntegerType(), True), StructField('_rescued_data', StringType(), True)])",
    'procedures': "StructType([StructField('START', TimestampType(), True), StructField('STOP', TimestampType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('SYSTEM', StringType(), True), StructField('CODE', LongType(), True), StructField('DESCRIPTION', StringType(), True), StructField('BASE_COST', DoubleType(), True), StructField('REASONCODE', LongType(), True), StructField('REASONDESCRIPTION', StringType(), True), StructField('_rescued_data', StringType(), True)])",
    'providers': "StructType([StructField('provider_id', StringType(), True), StructField('ORGANIZATION', StringType(), True), StructField('NAME', StringType(), True), StructField('GENDER', StringType(), True), StructField('SPECIALITY', StringType(), True), StructField('ADDRESS', StringType(), True), StructField('CITY', StringType(), True), StructField('STATE', StringType(), True), StructField('ZIP', IntegerType(), True), StructField('LAT', DoubleType(), True), StructField('LON', DoubleType(), True), StructField('ENCOUNTERS', IntegerType(), True), StructField('PROCEDURES', IntegerType(), True), StructField('_rescued_data', StringType(), True)])",
    'supplies': "StructType([StructField('DATE', DateType(), True), StructField('PATIENT', StringType(), True), StructField('ENCOUNTER', StringType(), True), StructField('CODE', IntegerType(), True), StructField('DESCRIPTION', StringType(), True), StructField('QUANTITY', IntegerType(), True), StructField('_rescued_data', StringType(), True)])"
}

# fix id column name
for key, val in schemas.items():
    if 'Id' in val:
        if key == 'patients':
            val = val.replace('Id', 'patient_id')
        elif key == 'careplans':
            val = val.replace('Id', 'careplan_id')
        elif key == 'claims':
            val = val.replace('Id', 'claim_id')
        elif key == 'claims_transactions':
            val = val.replace('ID', 'claims_transaction_id')
        elif key == 'encounters':
            val = val.replace('Id', 'encounter_id')
        elif key == 'imaging_studies':
            val = val.replace('Id', 'imaging_study_id')
        elif key == 'organizations':
            val = val.replace('Id', 'organization_id')
        elif key == 'payers':
            val = val.replace('Id', 'payer_id')
        elif key == 'providers':
            val = val.replace('Id', 'provider_id')

        schemas[key] = val

# fix column names
for key, val in schemas.items():
    if 'PATIENT' in val:
        val = val.replace('PATIENT', 'patient_id')
    if 'ENCOUNTER' in val:
        val = val.replace('ENCOUNTER', 'encounter_id')
    schemas[key] = val

print(schemas)
