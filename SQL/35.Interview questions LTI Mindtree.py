# Databricks notebook source
emp_data = [
    ("Manish", 1, 75000),
    ("Raghav", 1, 85000),
    ("surya",  1, 80000),
    ("virat",  2, 70000),
    ("rohit",  2, 75000),
    ("jadeja", 3, 85000),
    ("anil",   3, 55000),
    ("sachin", 3, 55000),
    ("zahir",  4, 60000),
    ("bumrah", 4, 65000)
]
schema = ["emp_name", "dept_id", "salary"]
emp_df = spark.createDataFrame(emp_data, schema)
emp_df.show()


dept_data = [
    (1, "DATA ENGINEER"),
    (2, "SALES"),
    (3, "SOFTWARE"),
    (4, "HR")
]
schema1 = ["dept_id", "dept_name"]
dept_df = spark.createDataFrame(dept_data, schema1)
dept_df.show()


# COMMAND ----------

# DBTITLE 1,1
emp_df.createOrReplaceTempView('emp') 
dept_df.createOrReplaceTempView('dept') 
# 1. WE HAVE TO FIND THE HIGHEST SALARY BASED ON EACH DEPARTMENT NAME -

# COMMAND ----------

# 2. WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING HIGHEST SALARY BASED ON EACH DEPARTMENT NAME

# COMMAND ----------

emp_df,dept_df
from pyspark.sql.functions import *
from pyspark.sql.window import Window

dfleft = emp_df.join(dept_df,emp_df.dept_id == dept_df.dept_id,'left')
# dfleft.display()
WindowSpec = Window.partitionBy('dept_name').orderBy(col('salary').desc())
dfrank = dfleft.withColumn('rank',rank().over(WindowSpec)).filter(col('rank')==1)
dfrank.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select *,d.dept_id as d_dept_id
# MAGIC   from emp e
# MAGIC   left join dept d
# MAGIC     on e.dept_id = d.dept_id
# MAGIC )
# MAGIC
# MAGIC select * from cte

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC   select *,d.dept_id as d_dept_id
# MAGIC   from emp e
# MAGIC   left join dept d
# MAGIC     on e.dept_id = d.dept_id
# MAGIC ),
# MAGIC cte2 as (
# MAGIC   select *, 
# MAGIC     rank() over(
# MAGIC       partition by dept_name 
# MAGIC       order by salary desc
# MAGIC     ) as rank
# MAGIC   from cte
# MAGIC )
# MAGIC select * 
# MAGIC from cte2 
# MAGIC where rank = 1

# COMMAND ----------

# DBTITLE 1,2
# MAGIC %sql
# MAGIC with cte as(
# MAGIC select * from emp e
# MAGIC left join dept d
# MAGIC on e.dept_id = d.dept_id
# MAGIC ),
# MAGIC cte2 as
# MAGIC (
# MAGIC   select *, rank() over(partition by d.dept_id order by salary) rank  from cte
# MAGIC )
# MAGIC
# MAGIC select *  from cte2 where rank =1
# MAGIC

# COMMAND ----------

# DBTITLE 1,1
from pyspark.sql.functions import max as _max,col

dfleft = emp_df.join(dept_df,emp_df.dept_id == dept_df.dept_id,'left')
# dfleft.show()
dfgroupdept = dfleft.groupby('dept_name').agg(_max(col('salary')))
dfgroupdept.show()


# COMMAND ----------

# DBTITLE 1,1
# MAGIC %sql
# MAGIC with cte as(
# MAGIC select * from emp e
# MAGIC left join dept d
# MAGIC on e.dept_id = d.dept_id
# MAGIC )
# MAGIC select emp_name,max(salary) from cte group by dept_name

# COMMAND ----------

# DBTITLE 1,1
# MAGIC %sql
# MAGIC with cte as(
# MAGIC select * from emp e
# MAGIC left join dept d
# MAGIC on e.dept_id = d.dept_id
# MAGIC ),
# MAGIC cte2 as(
# MAGIC select * , rank() over(partition by dept_name order by salary desc) rank from cte
# MAGIC )
# MAGIC select * from cte2 where rank = 1

# COMMAND ----------

#  WE HAVE 2 TABLE EMPLOYEE AND DEPARTMENT TABLE IS GIVEN
# 1. WE HAVE TO FIND THE HIGHEST SALARY BASED ON EACH DEPARTMENT NAME -
# 2. WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING HIGHEST SALARY BASED ON EACH DEPARTMENT NAME
# 3. WE HAVE TO FIND THE LOWEST SALARY BASED ON EACH DEPARTMENT NAME
# 4. WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING LOWEST SALARY BASED ON EACH DEPARTMENT NAME

# COMMAND ----------

# 3. WE HAVE TO FIND THE LOWEST SALARY BASED ON EACH DEPARTMENT NAME
from pyspark.sql.functions import min as _min

emp_df,dept_df
dfleft = emp_df.join(dept_df,emp_df.dept_id == dept_df.dept_id , 'left')
dfagg = dfleft.groupby('dept_name').agg(_min(col('salary')).alias('min_salary'))
display(dfagg)
# display(dfleft)

# COMMAND ----------


