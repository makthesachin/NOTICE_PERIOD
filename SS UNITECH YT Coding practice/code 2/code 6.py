# Databricks notebook source
# Input:
# | Country1 | Country2 | Country3 |
# | -------- | -------- | -------- |
# | India    | NULL     | China    |
# | NULL     | Russia   | NULL     |
# | NULL     | NULL     | China    |

# Output

# | Country |
# | ------- |
# | India   |
# | Russia  |
# | China   |

from pyspark.sql import SparkSession

data = [
    ("India", None, "China"),
    (None, "Russia", None),
    (None, None, "China")
]

columns = ["Country1", "Country2", "Country3"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import coalesce
df1 = df.select(coalesce('Country1','Country2','Country3').alias('Country'))
df1.show(truncate=False)
