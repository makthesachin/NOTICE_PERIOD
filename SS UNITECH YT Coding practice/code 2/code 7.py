# Databricks notebook source
# | customer_id | order_id | order_date | amount |
# | ----------- | -------- | ---------- | ------ |
# | 101         | A001     | 2024-01-10 | 250    |
# | 101         | A002     | 2024-02-12 | 300    |
# | 102         | B001     | 2024-03-05 | 180    |
# | 103         | C001     | 2024-01-15 | 400    |
# | 103         | C002     | 2024-04-20 | 420    |

# | customer_id | latest_order_date |
# | ----------- | ----------------- |
# | 101         | 2024-02-12        |
# | 102         | 2024-03-05        |
# | 103         | 2024-04-20        |


from pyspark.sql import SparkSession

data = [
    (101, "A001", "2024-01-10", 250),
    (101, "A002", "2024-02-12", 300),
    (102, "B001", "2024-03-05", 180),
    (103, "C001", "2024-01-15", 400),
    (103, "C002", "2024-04-20", 420)
]

columns = ["customer_id", "order_id", "order_date", "amount"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.partitionBy('customer_id').orderBy(col('Order_date').desc())
dfwindow = df.withColumn('rank',rank().over(w))
dfwindow.show(truncate=True)

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.partitionBy('customer_id').orderBy(col('Order_date').desc())
dfwindow = df.withColumn('rank',rank().over(w))
# dfwindow.show(truncate=True)
dfwindow1 = dfwindow.select('customer_id','order_date').where(col('rank')==1)
dfwindow1.show()
