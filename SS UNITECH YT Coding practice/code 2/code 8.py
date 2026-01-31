# Databricks notebook source
# 120000+8000+12000+60000
# 120000/200000*100
# 8000/200000*100
# 12000/100*100
60000/200000*100

# COMMAND ----------

# | product_id | product_name | revenue |
# | ---------- | ------------ | ------- |
# | 101        | Laptop       | 120000  |
# | 102        | Mouse        | 8000    |
# | 103        | Keyboard     | 12000   |
# | 104        | Monitor      | 60000   |


# | product_name | revenue | percentage_contribution |
# | ------------ | ------- | ----------------------- |
# | Laptop       | 120000  | 58.25                   |
# | Mouse        | 8000    | 3.88                    |
# | Keyboard     | 12000   | 5.83                    |
# | Monitor      | 60000   | 29.12                   |


data = [
    (101, "Laptop", 120000),
    (102, "Mouse", 8000),
    (103, "Keyboard", 12000),
    (104, "Monitor", 60000)
]

columns = ["product_id", "product_name", "revenue"]
df_q8 = spark.createDataFrame(data, columns)
df_q8.show()

# COMMAND ----------

df1 = df_q8.withColumn('percentage_contribution',col('revenue')/ total_revenue * 100).drop('product_id')
df1.show()
