# Databricks notebook source
# | emp_name | salary |
# | -------- | ------ |
# | A        | 120000 |
# | B        | 65000  |
# | C        | 35000  |

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('df').getOrCreate()
schema = [('A',120000),('B',65000),('C',35000)]
data = ['emp_name','salary']

df = spark.createDataFrame(schema,data)
df.show()
df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,
# MAGIC case 
# MAGIC when salary > 100000 then 'high'
# MAGIC when salary >50000 then 'medium'
# MAGIC when salary < 40000 then 'less'
# MAGIC else 'veryless'
# MAGIC end as standard_salary
# MAGIC from df

# COMMAND ----------

data = [
    (1, 'Goa'),
    (2, None),
    (3, 'Delhi')
]
columns = ['order_id', 'city']

df = spark.createDataFrame(data, columns)
display(df)
df.createOrReplaceTempView('df')

# COMMAND ----------

from pyspark.sql.functions import col, when

df = df.withColumn(
    "city_",
    when(col("city").isNull(), "null city")
    .when(col("city").isNotNull(), "not null city")
    .otherwise(col("city"))
)
df = df.withColumn("order_id", col("order_id"))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT df.*,
# MAGIC     order_id,
# MAGIC     CASE
# MAGIC         WHEN city IS NULL THEN 'null city'
# MAGIC         when city is not null then 'not null city'
# MAGIC         ELSE city
# MAGIC     END AS city_
# MAGIC FROM df 
# MAGIC

# COMMAND ----------

data = [
    (1, '2025-01-01'),
    (2, '2023-01-01')
]
columns = ['order_id', 'order_date']

df = spark.createDataFrame(data, columns)
display(df)
df.createOrReplaceTempView('df')

# COMMAND ----------

from pyspark.sql.functions import col, current_date, expr, when

df = df.withColumn(
    "order_status",
    when(
        col("order_date") >= expr("date_sub(current_date(), 400)"),
        "Recent"
    ).otherwise("Old")
)
df = df.withColumn("order_id", col("order_id"))
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC     order_id,
# MAGIC     CASE
# MAGIC         WHEN order_date >= CURRENT_DATE - INTERVAL '400' DAY
# MAGIC         THEN 'Recent'
# MAGIC         ELSE 'Old'
# MAGIC     END AS order_status
# MAGIC FROM df;
# MAGIC
