# Databricks notebook source
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



