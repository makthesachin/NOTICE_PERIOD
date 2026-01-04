# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,IntType,ArrayType,BooleanType,StringType

df = spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','json')\
    .option('cloudFiles.schemaLocation','/Volumes/data_dev/autoloader/bronze/destination/schema/')\
    .option()

# COMMAND ----------

# DBTITLE 1,autoloader read
from pyspark.sql.types import StructType, StructField, ArrayType, BooleanType, LongType, StringType

schema = StructType([
    StructField("data", ArrayType(
        StructType([
            StructField("active", BooleanType(), True),
            StructField("id", LongType(), True),
            StructField("location", StringType(), True),
            StructField("name", StringType(), True),
            StructField("role", StringType(), True)
        ])
    ), True)
])

df = spark.readStream.format('cloudFiles')\
    .schema(schema)\
    .option('cloudFiles.format','json')\
    .option('cloudFiles.schemaLocation','/Volumes/data_dev/autoloader/bronze/destination/schema/')\
    .option('cloudFiles.schemaEvolutionMode','rescue')\
    .load('/Volumes/data_dev/autoloader/bronze/raw/')

# COMMAND ----------

# DBTITLE 1,autoloader write
df.writeStream.format('delta')\
    .outputMode('append')\
    .option('checkpointLocation','/Volumes/data_dev/autoloader/bronze/destination/checkpoint/')\
    .option('mergeSchema','true')\
    .trigger(once = True)\
    .start('/Volumes/data_dev/autoloader/bronze/destination/data/')

# COMMAND ----------

dfwritten = spark.read.format('delta').load('/Volumes/data_dev/autoloader/bronze/destination/data/')
display(dfwritten)
