# Databricks notebook source
# Input Table
# | customer_id | transaction_date |
# | ----------- | ---------------- |
# | customer1   | 01/05           |
# | customer1   | 01/25            |
# | customer1   | 02/10            |
# | customer2   | 01/15            |
# | customer2   | 02/05            |
# | customer3   | 01/01            |
# | customer3   | 01/20            |
# | customer3   | 02/01            |
# | customer3   | 02/15            |

# Expected Table:
# | customer_id | latest_transaction_date |
# | ----------- | ----------------------- |
# | customer1   | 02/10                   |
# | customer2   | 02/05                   |
# | customer3   | 02/15                   |

data = [
    ("customer1", "01/05"),
    ("customer1", "01/25"),
    ("customer1", "02/10"),
    ("customer2", "01/15"),
    ("customer2", "02/05"),
    ("customer3", "01/01"),
    ("customer3", "01/20"),
    ("customer3", "02/01"),
    ("customer3", "02/15")
]

columns = ["customer_id", "transaction_date"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(select *, date_format(to_date(transaction_date,'MM/dd'),'MMMM-MM') as month,day(to_date(transaction_date,'MM/dd')) as day,concat_ws('.',month,day) as day_concat from df),
# MAGIC cte2 as (
# MAGIC select *, row_number() over(partition by customer_id order by day_concat desc) as rank from cte)
# MAGIC select * from cte2 

# COMMAND ----------

# DBTITLE 1,Untitled
M%sql
with cte as(select *, month(to_date(transaction_date,'MM/dd')) as month,day(to_date(transaction_date,'MM/dd')) as day,concat_ws('.',month,day) as day_concat from df),
cte2 as (
select *, row_number() over(partition by customer_id order by day_concat desc) as rank from cte)
select customer_id,transaction_date from cte2 where rank = 1

# COMMAND ----------

# DBTITLE 1,final code
# MAGIC %sql
# MAGIC with cte as(select *, month(to_date(transaction_date,'MM/dd')) as month,day(to_date(transaction_date,'MM/dd')) as day,concat_ws('.',month,day) as day_concat from df),
# MAGIC cte2 as (
# MAGIC select *, row_number() over(partition by customer_id order by day_concat desc) as rank from cte)
# MAGIC select customer_id,transaction_date from cte2 where rank = 1
