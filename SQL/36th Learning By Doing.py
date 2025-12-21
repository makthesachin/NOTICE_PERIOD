# Databricks notebook source
# DBTITLE 1,1
from pyspark.sql.functions import *

data = [
    (1, ["mobile", "PC", "Tab"]),
    (2, ["mobile", "PC"]),
    (3, ["Tab", "Pen"])
]

schema = ["customer_id", "product_purchase"]

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

# DBTITLE 1,1
from pyspark.sql.functions import *
dfflatten = df.select('*',explode('product_purchase').alias('product_purchase_explode')).show()

# COMMAND ----------

# DBTITLE 1,1
from pyspark.sql.functions import *
dfflatten = df.withColumn('product_purchase_flatten',explode('product_purchase')).drop('product_purchase').show()

# COMMAND ----------

# DBTITLE 1,2
data = [
    (1, 'yes', None, None),
    (2, None, 'yes', None),
    (3, 'No', None, 'yes')
]

schema = [
    'customer_id',
    'device_using1',
    'device_using2',
    'device_using3'
]

df1 = spark.createDataFrame(data, schema)
df1.show()

# COMMAND ----------

from pyspark.sql.functions import col, coalesce

df = (
  df1.select(
    "*",
    coalesce(
      col("device_using1"),
      col("device_using2"),
      col("device_using3")
    ).alias("device_using")
  )
  .drop(
    "device_using1",
    "device_using2",
    "device_using3"
  )
)
display(df)

# COMMAND ----------

df = df1.select("*",coalesce(col('device_using1'),col('device_using2'),col('device_using3').alias('device_using')).drop('device_using1','device_using2','device_using3'))
display(df)

# COMMAND ----------


