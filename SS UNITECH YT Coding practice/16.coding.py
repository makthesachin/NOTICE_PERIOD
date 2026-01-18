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

# COMMAND ----------

pivot_df = df.groupBy("EmpName", "EmpId") \
    .pivot("Skills") \
    .agg(count("*"))

# COMMAND ----------

from pyspark.sql.functions import count, sum,col
from pyspark.sql.window import Window

dfgroup = dept_gender_df.groupby('DeptName', 'Gender').agg(count('*').alias('Gender_Counter'))
w = Window.partitionBy('DeptName')
dftotal = dfgroup.withColumn('Total_Gender_Count', sum('Gender_Counter').over(w))

dfpivot = dftotal.groupBy('DeptName').pivot('Gender').agg(count(col("Gender")))
# dfpivot = dftotal.groupBy('DeptName').pivot('Gender').agg(count('*'))
display(dfpivot)
# display(dftotal)

# COMMAND ----------

# DBTITLE 1,test
from pyspark.sql.functions import count, col, sum
from pyspark.sql.window import Window

dfgroup = dept_gender_df.groupby('DeptName', 'Gender').agg(count('*').alias('Gender_Counter'))
w = Window.partitionBy('DeptName')
dftotal = dfgroup.withColumn('Total_Gender_Count', sum('Gender_Counter').over(w))

pivot_df = dftotal.groupBy('DeptName') \
    .pivot('Gender') \
    .agg(
        sum('Gender_Counter').alias('Gender_Counter'),
        sum('Total_Gender_Count').alias('Total_Gender_Count')
    )

display(pivot_df)

# COMMAND ----------

from pyspark.sql.functions import count, col,sum
from pyspark.sql.window import Window

dfgroup = dept_gender_df.groupby('DeptName','Gender').agg(count('*').alias('Gender_Counter'))
w = Window.partitionBy('DeptName')
dftotal = dfgroup.withColumn('Total_Gender_Count',sum('Gender_Counter').over(w))
display(dftotal)

dftotal = dfgroup.groupby('DeptName').agg(sum('Gender_Counter').alias('Total_Gender_count'))
display(dfgroup)
display(dftotal)

# COMMAND ----------



