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
df.show()
