# Databricks notebook source
# Q4 – Problem Statement

# Handle corrupt/bad records while loading a CSV file into Spark DataFrame (without filtering after read).


# Input table
# Emp_no,Emp_name,Department
# 101,Murugan,HealthCare
# Invalid Entry, Description: Bad Record entry
# 102,Kannan,Finance
# 103,Mani,IT
# Connection lost, Description: Poor Connection
# 104,Pavan,HR
# Bad Record, Description: Corrupt record

# Expected_output table
# | Emp_no | Emp_name | Department |
# | ------ | -------- | ---------- |
# | 101    | Murugan  | HealthCare |
# | 102    | Kannan   | Finance    |
# | 103    | Mani     | IT         |
# | 104    | Pavan    | HR         |


data = [
    ("101,Murugan,HealthCare",),
    ("Invalid Entry, Description: Bad Record entry",),
    ("102,Kannan,Finance",),
    ("103,Mani,IT",),
    ("Connection lost, Description: Poor Connection",),
    ("104,Pavan,HR",),
    ("Bad Record, Description: Corrupt record",)
]

df_raw = spark.createDataFrame(data, ["value"])
df_raw.show(truncate=False)

# COMMAND ----------


