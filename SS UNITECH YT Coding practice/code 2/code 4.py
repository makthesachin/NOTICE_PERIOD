# Databricks notebook source
# Input:
# | EmpId | Date       | Category | Amount |
# | ----: | ---------- | -------- | ------ |
# |   101 | 2025-01-12 | Travel   | 1200   |
# |   102 | 2025-01-15 | Food     | 800    |
# |   103 | 2025-01-18 | Travel   | 900    |
# |   101 | 2025-02-09 | Office   | 700    |
# |   104 | 2025-02-10 | Travel   | 1500   |
# |   102 | 2025-02-18 | Food     | 1300   |
# |   105 | 2025-02-20 | Supplies | 1100   |
# |   101 | 2025-03-05 | Travel   | 2000   |
# |   102 | 2025-03-08 | Food     | 2200   |
# |   103 | 2025-03-15 | Travel   | 1800   |

# Output
# | EmpId | Date       | Category | Amount |
# | ----: | ---------- | -------- | ------ |
# |   101 | 2025-01-12 | Travel   | 1200   |
# |   103 | 2025-01-18 | Travel   | 900    |
# |   104 | 2025-02-10 | Travel   | 1500   |
# |   101 | 2025-03-05 | Travel   | 2000   |
# |   103 | 2025-03-15 | Travel   | 1800   |
# |   102 | 2025-01-15 | Food     | 800    |
# |   102 | 2025-02-18 | Food     | 1300   |
# |   102 | 2025-03-08 | Food     | 2200   |
# |   101 | 2025-02-09 | Office   | 700    |
# |   105 | 2025-02-20 | Supplies | 1100   |

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (101, "2025-01-12", "Travel", 1200),
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

columns = ["EmpId", "Date", "Category", "Amount"]
df = spark.createDataFrame(data, columns)

df.show()

