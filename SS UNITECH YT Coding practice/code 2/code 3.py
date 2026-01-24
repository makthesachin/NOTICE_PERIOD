# Databricks notebook source
# Input:
# | EmpId | Date      | Category | Amount |
# | ----: | --------- | -------- | -----: |
# |   101 | 1/2/2025  | Travel   |   1200 |
# |   102 | 1/15/2025 | Food     |    800 |
# |   103 | 1/18/2025 | Travel   |    900 |
# |   101 | 2/9/2025  | Office   |    700 |
# |   104 | 2/10/2025 | Travel   |   1500 |
# |   102 | 2/18/2025 | Food     |   1300 |
# |   105 | 2/20/2025 | Supplies |   1100 |
# |   101 | 3/5/2025  | Travel   |   2000 |
# |   102 | 3/8/2025  | Food     |   2200 |
# |   103 | 3/15/2025 | Travel   |   1800 |

# Output:
# | Month   | EmpId | Total_Amount |
# | ------- | ----: | -----------: |
# | 2025-01 |   101 |         1200 |
# | 2025-01 |   103 |          900 |
# | 2025-02 |   104 |         1500 |
# | 2025-02 |   102 |         1300 |
# | 2025-03 |   102 |         2200 |
# | 2025-03 |   101 |         2000 |


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sales_data = [
    (101, "2025-01-02", "Travel", 1200),
    (102, "2025-01-15", "Food", 800),
    (103, "2025-01-18", "Travel", 900),
    (101, "2025-02-09", "Office", 700),
    (104, "2025-02-10", "Travel", 1500),
    (102, "2025-02-18", "Food", 1300),
    (105, "2025-02-20", "Supplies", 1100),
    (101, "2025-03-05", "Travel", 2000),
    (102, "2025-03-08", "Food", 2200),
    (103, "2025-03-15", "Travel", 1800)
]

sales_df = spark.createDataFrame(
    sales_data,
    ["EmpId", "Date", "Category", "Amount"]
)

sales_df.show()

