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

# +--------+---+----+
# |DeptName|  F|   M|
# +--------+---+----+
# |      IT|  1|   3|
# |      HR|  5|NULL|
# |   Sales|  2|   4|
# +--------+---+----+

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

# DBTITLE 1,Cell 5
from pyspark.sql.functions import count, row_number
from pyspark.sql.window import Window

w = Window.partitionBy('DeptName','Gender').orderBy('Gender')
df1 = dept_gender_df.withColumn('row_number_rank', count('Gender').over(w))
df1.show()
df2 = df1.groupby('DeptName','Gender').count()
# df2.show()
df3 = df2.groupby('DeptName').pivot('Gender').agg(sum('count'))
df3.show()

# COMMAND ----------

from pyspark.sql.functions import count

df1 = dept_gender_df.groupBy('DeptName').agg(count('Gender').alias('TotalEmp'))
display(df1)

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import sum

df1 = dept_gender_df.groupBy('DeptName').agg(sum('Gender'))
df1.show()

# COMMAND ----------



# COMMAND ----------



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
