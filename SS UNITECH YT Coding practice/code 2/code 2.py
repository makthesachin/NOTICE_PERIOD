# Databricks notebook source
# | EmpId | EmpName | Salary | DeptName   |
# | ----: | ------- | -----: | ---------- |
# |     1 | A       |   1200 | Finance    |
# |     2 | B       |   1800 | Finance    |
# |     3 | C       |   2700 | Finance    |
# |     4 | D       |   3200 | Marketing  |
# |     5 | E       |   2100 | Marketing  |
# |     6 | F       |   1500 | Marketing  |
# |     7 | G       |   4200 | Operations |
# |     8 | H       |   4200 | Operations |
# |     9 | I       |   1100 | Operations |
# |    10 | J       |   2500 | Operations |

# | EmpId | EmpName | Salary | DeptName   |
# | ----: | ------- | -----: | ---------- |
# |     3 | C       |   2700 | Finance    |
# |     4 | D       |   3200 | Marketing  |
# |     7 | G       |   4200 | Operations |
# |     8 | H       |   4200 | Operations |

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

emp_data = [
    (1, "A", 1200, "Finance"),
    (2, "B", 1800, "Finance"),
    (3, "C", 2700, "Finance"),
    (4, "D", 3200, "Marketing"),
    (5, "E", 2100, "Marketing"),
    (6, "F", 1500, "Marketing"),
    (7, "G", 4200, "Operations"),
    (8, "H", 4200, "Operations"),
    (9, "I", 1100, "Operations"),
    (10, "J", 2500, "Operations")
]

emp_df = spark.createDataFrame(
    emp_data,
    ["EmpId", "EmpName", "Salary", "DeptName"]
)

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import col
df = emp_df.filter(col('Salary')>=2700)
df.show(truncate = False)

# COMMAND ----------


