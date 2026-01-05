# Databricks notebook source
# | DeptName | Gender |
# | -------- | ------ |
# | IT       | M      |
# | IT       | F      |
# | IT       | M      |
# | IT       | M      |
# | HR       | F      |
# | HR       | F      |
# | HR       | F      |
# | HR       | F      |
# | HR       | F      |
# | Sales    | M      |
# | Sales    | M      |
# | Sales    | F      |
# | Sales    | M      |
# | Sales    | M      |
# | Sales    | F      |


# COMMAND ----------

# | DeptName | TotalEmp | MaleEmp | FemaleEmp |
# | -------- | -------: | ------: | --------: |
# | IT       |        4 |       3 |         1 |
# | HR       |        5 |       0 |         5 |
# | Sales    |        6 |       4 |         2 |


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dept_gender_data = [
    ("IT", "M"),
    ("IT", "F"),
    ("IT", "M"),
    ("IT", "M"),

    ("HR", "F"),
    ("HR", "F"),
    ("HR", "F"),
    ("HR", "F"),
    ("HR", "F"),

    ("Sales", "M"),
    ("Sales", "M"),
    ("Sales", "F"),
    ("Sales", "M"),
    ("Sales", "M"),
    ("Sales", "F")
]

dept_gender_df = spark.createDataFrame(
    dept_gender_data,
    ["DeptName", "Gender"]
)

dept_gender_df.show()

