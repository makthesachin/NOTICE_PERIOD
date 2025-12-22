# Databricks notebook source
/Volumes/workspace/bronze_history/files_dump/singleLine.json

# COMMAND ----------

dfsinglejson = spark.read.format('json').load('/Volumes/workspace/bronze_history/files_dump/singleLine.json')
display(dfsinglejson)

# COMMAND ----------

dfMultiline = spark.read.format('json').load('/Volumes/workspace/bronze_history/files_dump/multilineJson.json',multiline=True)
display(dfMultiline)

# COMMAND ----------

from pyspark.sql.functions import explode,flatten,col

dfnew = dfMultiline.withColumn('skills_explode',explode('skills')).drop('skills') \
                   .withColumn('address_city',col('address.city')) \
                   .withColumn('address_pincode',col('address.pincode'))\
                    .withColumn('address_state',col('address.state')).drop('address')
display(dfnew)

# COMMAND ----------

display(dbutils.fs.ls('/Volumes/workspace/bronze_history/files_dump/multilineJson.json'))

# COMMAND ----------

json = 
    {
  "employee": {
    "id": 1,
    "name": "Sachin"
  },
  "skills": ["Spark", "SQL", "Python"],
  "attributes": {
    "location": "Hyderabad",
    "shift": "day"
  }
}

print(datatype(json))


# COMMAND ----------

json = {
    "employee": {
        "id": 1,
        "name": "Sachin"
    },
    "skills": [
        "Spark",
        "SQL",
        "Python"
    ],
    "attributes": {
        "location": "Hyderabad",
        "shift": "day"
    }
}

df_json = spark.createDataFrame([json])
display(df_json)

# COMMAND ----------


