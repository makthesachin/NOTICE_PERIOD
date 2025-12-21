# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

# Employees schema
emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("dept_id", IntegerType(), True)
])

# Employees data
emp_data = [
    (1, 10),
    (2, None),
    (3, 20),
    (4, None)
]

employees_df = spark.createDataFrame(emp_data, emp_schema)

# Departments schema
dept_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True)
])

# Departments data
dept_data = [
    (10, "HR"),
    (20, "IT"),
    (None, "UNKNOWN")
]

departments_df = spark.createDataFrame(dept_data, dept_schema)

employees_df.show()
departments_df.show()


# COMMAND ----------

df_inner = employees_df.join(departments_df,employees_df.dept_id == departments_df.dept_id,'inner')
display(df_inner)

# COMMAND ----------

df_left = employees_df.join(departments_df,employees_df.dept_id == departments_df.dept_id,'left')
display(df_left)

# COMMAND ----------

df_right= employees_df.join(departments_df,employees_df.dept_id == departments_df.dept_id,'right')
display(df_right)

# COMMAND ----------

df_full= employees_df.join(departments_df,employees_df.dept_id == departments_df.dept_id,'full')
display(df_full)

# COMMAND ----------

inner join - 3 , 2
left join emp - 4 , 4
right join - 4 , 3
full join - 7 , 5
