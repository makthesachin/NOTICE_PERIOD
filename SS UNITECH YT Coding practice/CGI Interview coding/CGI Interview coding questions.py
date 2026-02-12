# Databricks notebook source
data = [
    (101, 50000, 2021),
    (101, 65000, 2022),
    (101, 72000, 2023),
    (102, 40000, 2022),
    (102, 55000, 2023),
    (103, 60000, 2022)
]
columns = ["EmpId", "Salary", "Year"]
df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------


