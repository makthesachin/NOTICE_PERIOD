# Databricks notebook source
# Input: 
# | EmpId | EmpName | Salary | DeptName |
# | ----- | ------- | ------ | -------- |
# | 1     | A       | 1000   | IT       |
# | 2     | B       | 1500   | IT       |
# | 3     | C       | 2500   | IT       |
# | 4     | D       | 3000   | HR       |
# | 5     | E       | 2000   | HR       |
# | 6     | F       | 1000   | HR       |
# | 7     | G       | 4000   | Sales    |
# | 8     | H       | 4000   | Sales    |
# | 9     | I       | 1000   | Sales    |
# | 10    | J       | 2000   | Sales    |

# output:
# | EmpId | EmpName | Salary | DeptName |
# | ----- | ------- | ------ | -------- |
# | 3     | C       | 2500   | IT       |
# | 4     | D       | 3000   | HR       |
# | 7     | G       | 4000   | Sales    |
# | 8     | H       | 4000   | Sales    |


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

emp_data = [
    (1, "A", 1000, "IT"),
    (2, "B", 1500, "IT"),
    (3, "C", 2500, "IT"),
    (4, "D", 3000, "HR"),
    (5, "E", 2000, "HR"),
    (6, "F", 1000, "HR"),
    (7, "G", 4000, "Sales"),
    (8, "H", 4000, "Sales"),
    (9, "I", 1000, "Sales"),
    (10, "J", 2000, "Sales")
]

emp_df = spark.createDataFrame(
    emp_data,
    ["EmpId", "EmpName", "Salary", "DeptName"]
)

emp_df.show()
emp_df.createOrReplaceTempView('emp_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, dense_rank() over(partition by deptname order by salary desc) as rank
# MAGIC from emp_df

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select *, dense_rank() over(partition by deptname order by salary desc) as rank
# MAGIC from emp_df)
# MAGIC select * from cte where rank = 1 
# MAGIC

# COMMAND ----------


