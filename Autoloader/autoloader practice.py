# Databricks notebook source
# MAGIC %sql 
# MAGIC list '/Volumes/data_dev/autoloader/bronze/destination/'
# MAGIC
# MAGIC -- /Volumes/data_dev/autoloader/bronze/destination/checkpoint/
# MAGIC -- /Volumes/data_dev/autoloader/bronze/destination/data/
# MAGIC -- /Volumes/data_dev/autoloader/bronze/destination/schema/
# MAGIC -- /Volumes/data_dev/autoloader/bronze/raw/sample_json1.json
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC list '/Volumes/data_dev/autoloader/bronze/'

# COMMAND ----------

fjson = spark.read.format('json').load('/Volumes/autoloader/dev_data/bronze/raw/sample_json1.json',multiline=True)
dfjson.display()

# COMMAND ----------

# DBTITLE 1,read autoloader
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
    .option('cloudFiles.schemaLocation','/Volumes/data_dev/autoloader/bronze/destination/checkpoint/')\
    .load('/Volumes/data_dev/autoloader/bronze/raw/')

# COMMAND ----------

# DBTITLE 1,write autoloader
df.writeStream.format('delta')\
    .outputMode('append')\
    .option('checkpointLocation','/Volumes/data_dev/autoloader/bronze/destination/checkpoint/')\
    .trigger(once = True)\
    .start('/Volumes/data_dev/autoloader/bronze/destination/data/')   

# COMMAND ----------

dfwritten= spark.read.format('delta').load('/Volumes/data_dev/autoloader/bronze/destination/data/')
display(dfwritten)

# COMMAND ----------


