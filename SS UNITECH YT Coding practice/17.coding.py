# Databricks notebook source
# | name   | phone        |
# | ------ | ------------ |
# | John   | 040-20215632 |
# | Joanne | 044-23651023 |
# | Tom    | 086-12456789 |

# | name   | stdcode | phonenum |
# | ------ | ------- | -------- |
# | John   | 040     | 20215632 |
# | Joanne | 044     | 23651023 |
# | Tom    | 086     | 12456789 |

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

phone_data = [
    ("John", "040-20215632"),
    ("Joanne", "044-23651023"),
    ("Tom", "086-12456789")
]

phone_df = spark.createDataFrame(
    phone_data,
    ["name", "phone"]
)

phone_df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import split

df = phone_df.withColumn('stdcode', split('phone', '-').getItem(0))\
    .withColumn('phonenum', split('phone', '-').getItem(1))\
    .drop('phone')
df.show()

# COMMAND ----------

df=phone_df.withColumn('stcode',split('phone','-').getItem(0))\
    .withColumn('phonenum',split('phone','-').getItem(1))\
    .drop('phone')

df.show()

# COMMAND ----------


