# Databricks notebook source
# | EmpId | EmpName | Department | Salary |
# | ----: | ------- | ---------- | ------ |
# |   101 | John    | HR         | 50000  |
# |   102 | Smith   | IT         | 70000  |
# |   103 | Alice   | Finance    | 60000  |
# |   104 | Bob     | IT         | 80000  |
# |   105 | David   | Finance    | 90000  |
# |   106 | Emma    | HR         | 70000  |

# | Salary |
# | ------ |
# | 90000  |
# | 80000  |
# | 70000  |

from pyspark.sql import SparkSession

data = [
    (101, "John", "HR", 50000),
    (102, "Smith", "IT", 70000),
    (103, "Alice", "Finance", 60000),
    (104, "Bob", "IT", 80000),
    (105, "David", "Finance", 90000),
    (106, "Emma", "HR", 70000)
]

columns = ["EmpId", "EmpName", "Department", "Salary"]

df = spark.createDataFrame(data, columns)
df.show(
    
)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
w = Window.orderBy(col('Salary').desc())
df_rank = df.withColumn('rank', row_number().over(w))
# df_rank.show()
df_rank = df_rank.filter(col('rank')<4)
df_rank.select('Salary').show()
