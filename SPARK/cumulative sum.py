# Databricks notebook source
from pyspark.sql import Row

data = [
    Row(id=1, revenue=100.0),
    Row(id=2, revenue=200.0),
    Row(id=3, revenue=150.0),
    Row(id=4, revenue=175.0),
    Row(id=5, revenue=220.0),
    Row(id=6, revenue=130.0)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC        ROW_NUMBER() OVER(PARTITION BY id order by id ) AS emp_rank
# MAGIC FROM df;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, sum(revenue) over(partition by id order by revenue desc)  as cumulative_sum
# MAGIC from df

# COMMAND ----------


