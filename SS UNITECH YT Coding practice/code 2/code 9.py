# Databricks notebook source
# It duplicate question i.e code 7- can be skipped
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

data = [
    (101, "A001", "2024-01-10", 250),
    (101, "A002", "2024-02-12", 300),
    (102, "B001", "2024-03-05", 180),
    (103, "C001", "2024-01-15", 400),
    (103, "C002", "2024-04-20", 420)
]

columns = ["customer_id", "order_id", "order_date", "amount"]
df_q9 = spark.createDataFrame(data, columns)
df_q9.show()
