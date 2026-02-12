# Databricks notebook source
# Input Table
# | customer_id | transaction_date      |
# | ----------- | --------------------- |
# | customer1   | 01/05/2026            |
# | customer1   | 01/25/2026            |
# | customer1   | 02/01/2026            |
# | customer2   | 01/15/2026            |
# | customer2   | 02/04/2026            |
# | customer3   | 01/01/2026            |
# | customer3   | 01/20/2026            |
# | customer3   | 02/01/2026            |
# | customer3   | 02/04/2026            |

# Expected Table: who did not make any transaction from 4 days back
# | customer_id | last_txn_date |
# | ----------- | ------------- |
# | customer1   | 02/01/2026    |
# | customer2   | 02/04/2026    |
# | customer3   | 02/04/2026    |

#who made transaction from 4 days
# | customer_id | last_txn_date |
# | ----------- | ------------- |
# | customer1   | 02/01/2026    |
# | customer2   | 02/04/2026    |
# | customer3   | 02/04/2026    |



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


