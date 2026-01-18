# Databricks notebook source
# | Month | Sales |
# | ----- | ----- |
# | Jan   | 1000  |
# | Feb   | 1200  |
# | Mar   | 1500  |


# | Month | Sales | Prev_Month_Sales |
# | ----- | ----- | ---------------- |
# | Jan   | 1000  | NULL             |
# | Feb   | 1200  | 1000             |
# | Mar   | 1500  | 1200             |


# COMMAND ----------

sales_data = [
    ("Jan", 1000),
    ("Feb", 1200),
    ("Mar", 1500)
]

sales_df = spark.createDataFrame(
    sales_data,
    ["Month", "Sales"]
)

sales_df.show()
sales_df.createOrReplaceTempView('df')

# COMMAND ----------

# DBTITLE 1,Cell 3
# sales_df
from pyspark.sql.functions import *
from pyspark.sql.window import Window

w = Window.orderBy('Sales')
lagdf = sales_df.withColumn('lagcolumn',lag('Sales').over(w))
loaddf = lagdf.withColumn('leadcolumn',lead('Sales').over(w))
loaddf.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC        LAG(Sales) OVER (
# MAGIC            ORDER BY to_date(Month, 'MMM')
# MAGIC        ) AS Prev_Month_Sales
# MAGIC FROM df;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,lag(Sales) over(Order by month) as Prev_Month_Sales from df
