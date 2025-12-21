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

employees_df.createOrReplaceTempView('emp')
departments_df.createOrReplaceTempView('dept')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp e
# MAGIC join dept d
# MAGIC on e.dept_id == d.dept_id 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp e
# MAGIC right join dept  d 
# MAGIC on e.dept_id == d.dept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp e
# MAGIC right join dept d
# MAGIC on e.dept_id == d.dept_id 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp e
# MAGIC full join dept d 
# MAGIC on e.dept_id == d.dept_id

# COMMAND ----------

# DBTITLE 1,3rd question
from pyspark.sql import Row

# df1 DataFrame
df1_data = [
    Row(col1=1),
    Row(col1=1),
    Row(col1=1),
    Row(col1=1),
    Row(col1=1),
    Row(col1=None),
    Row(col1=None)
]
df1 = spark.createDataFrame(df1_data)
df1.show()
df2.show()

# df2 DataFrame
df2_data = [
    Row(col2=1),
    Row(col2=1),
    Row(col2=1),
    Row(col2=2),
    Row(col2=None)
]
df2 = spark.createDataFrame(df2_data)

# COMMAND ----------

df1.createOrReplaceTempView('df1')
df2.createOrReplaceTempView('df2')

# COMMAND ----------

# DBTITLE 1,ineer join
# MAGIC
# MAGIC %sql
# MAGIC select * from df1
# MAGIC inner join df2
# MAGIC on col1 = col2

# COMMAND ----------

# DBTITLE 1,full join
# MAGIC %sql
# MAGIC select * from df1
# MAGIC full join df2
# MAGIC on col1 = col2

# COMMAND ----------

# DBTITLE 1,right join
# MAGIC %sql
# MAGIC select * from df1
# MAGIC right join df2
# MAGIC on col1 = col2

# COMMAND ----------

# DBTITLE 1,left join
# MAGIC %sql
# MAGIC select * from df1
# MAGIC left join df2
# MAGIC on col1 = col2

# COMMAND ----------

inner join - 15
left jon - 17
right join - 17
full join - 19


# COMMAND ----------

from pyspark.sql import Row

df1_data = [
    Row(col1=1),
    Row(col1=2),
    Row(col1=None)
]
df1 = spark.createDataFrame(df1_data)
df1.show()

df2_data = [
    Row(col2=1),
    Row(col2=1)
]
df2 = spark.createDataFrame(df2_data)
df2.show()

# COMMAND ----------

df1.createOrReplaceTempView('df1')
df2.createOrReplaceTempView('df2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df1
# MAGIC inner join df2
# MAGIC on col1==col2

# COMMAND ----------

df_inner = df1.join(df2,df1.col1==df2.col2,'full')
df_inner.show()

# COMMAND ----------

inner join - 2 , 1
left join - 4 , 3
right join - 2 , 1
full join - 4 , 3

# COMMAND ----------

from pyspark.sql import Row

# table1 DataFrame
table1_data = [
    Row(col1=1),
    Row(col1=1),
    Row(col1=1),
    Row(col1=2),
    Row(col1=3),
    Row(col1=3),
    Row(col1=3)
]
table1_df = spark.createDataFrame(table1_data)

# table2 DataFrame
table2_data = [
    Row(col2=1),
    Row(col2=1),
    Row(col2=2),
    Row(col2=2),
    Row(col2=4),
    Row(col2=None)
]
table2_df = spark.createDataFrame(table2_data)


# COMMAND ----------

# DBTITLE 1,2nd question


# COMMAND ----------

table1_df.createOrReplaceTempView('table1')

# COMMAND ----------

table2_df.createOrReplaceTempView('table2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table2

# COMMAND ----------

# DBTITLE 1,inner join 
# MAGIC %sql
# MAGIC select * from table1
# MAGIC inner join table2
# MAGIC on col1 = col2

# COMMAND ----------

# DBTITLE 1,left join
# MAGIC %sql
# MAGIC select * from table1
# MAGIC left join table2
# MAGIC on col1 = col2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table1
# MAGIC right join table2
# MAGIC on col1 = col2

# COMMAND ----------

inner join - 3
left join table1 - 7
right join - 6
full join - 10

# COMMAND ----------


