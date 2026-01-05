# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dept_data = [
    (1, "IT"),
    (2, "Sales")
]
dept_df = spark.createDataFrame(
    dept_data,
    ["DeptID", "DeptName"]
)
dept_df.show()

# COMMAND ----------

from datetime import datetime

emp_data = [
    (100, "Raj", None, 1, "2023-04-01", 50000),
    (200, "Venki", 100, 1, "2023-04-13", 4000),
    (200, "Venki", 100, 1, "2023-04-01", 4500),
    (200, "Venki", 100, 1, "2023-05-14", 4020)
]
emp_df = spark.createDataFrame(
    emp_data,
    ["EmpID", "EmpName", "MgrID", "DeptID", "Sal_Date", "Sal"]
)
emp_df.show()

# COMMAND ----------

# 🔹 Task 1
# 1.DeptName,Manager Name,Employee Name,Salary Year,Salary Month,Total Monthly Salary
# 🔹 Task 2
# 2.Find 7th highest salary (or Nth highest – interviewer may change N)

# COMMAND ----------

from pyspark.sql.functions import col

dfself = emp_df.alias('df1').join(
    dept_df.alias('df2'),
    col('df1.EmpID') == col('df2.EmpID'),
    'inner'
)
display(dfself)
