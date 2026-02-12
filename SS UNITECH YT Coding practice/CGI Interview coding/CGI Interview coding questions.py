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

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,row_number() over(partition by Empid order by Salary desc,Year desc) as rank from df 

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select *,row_number() over(partition by Empid order by Salary desc ,Year desc) as rank from df) 
# MAGIC select * from cte where rank = 1

# COMMAND ----------

# DBTITLE 1,duplicate trap
data_new = [
    (1, 90000),
    (2, 80000),
    (3, 80000),
    (4, 70000),
    (5, 60000)
]
columns_new = ["EmpId", "Salary"]
df_new = spark.createDataFrame(data_new, columns_new)
display(df_new)

# COMMAND ----------

df_new.createOrReplaceTempView('df_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,row_number() over(order by Salary desc) as rank from df_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,rank() over(order by Salary desc) as rank from df_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,dense_rank() over(order by Salary desc) as rank from df_new

# COMMAND ----------


