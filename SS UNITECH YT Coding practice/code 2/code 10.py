# Databricks notebook source
# | emp_id | month | sales |
# | ------ | ----- | ----- |
# | 101    | Jan   | 5000  |
# | 102    | Jan   | 8000  |
# | 103    | Jan   | 8000  |
# | 104    | Jan   | 4000  |

# | emp_id | month | sales | rank | dense_rank | row_number |
# | ------ | ----- | ----- | ---- | ---------- | ---------- |
# | 102    | Jan   | 8000  | 1    | 1          | 1          |
# | 103    | Jan   | 8000  | 1    | 1          | 2          |
# | 101    | Jan   | 5000  | 3    | 2          | 3          |
# | 104    | Jan   | 4000  | 4    | 3          | 4          |


data = [

    (101, "Jan", 5000),
    (102, "Jan", 8000),
    (103, "Jan", 8000),
    (104, "Jan", 4000)
]

columns = ["emp_id", "month", "sales"]
df_q10 = spark.createDataFrame(data, columns)
df_q10.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.orderBy(col('sales').desc())
df1 =df_q10.withColumn('rank',rank().over(w)).withColumn('row_number',row_number().over(w)).withColumn('dense_rank',dense_rank().over(w))
df1.show(truncate=False)
