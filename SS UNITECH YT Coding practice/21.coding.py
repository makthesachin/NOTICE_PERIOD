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
