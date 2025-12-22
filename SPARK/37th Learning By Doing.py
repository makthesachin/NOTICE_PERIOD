# Databricks notebook source
from pyspark.sql import Row

data = [
    ("Manish", Row(street="123 St", city="Delhi")),
    ("Ram",    Row(street="456 St", city="Mumbai"))
]

schema = ["name", "address"]

df = spark.createDataFrame(data, schema)
print(df.printSchema())
# df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col
display(df.select("*",col('address.street'),col('address.city')))

# COMMAND ----------

display(df)

# COMMAND ----------


