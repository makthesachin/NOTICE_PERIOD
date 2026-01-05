# Databricks notebook source
# | SOID | SODate     | ItemId | ItemQty | ItemValue |
# | ---: | ---------- | ------ | ------: | --------: |
# |    1 | 01-01-2024 | I1     |      10 |      1000 |
# |    2 | 15-01-2024 | I2     |      20 |      2000 |
# |    3 | 01-02-2024 | I3     |      10 |      1500 |
# |    4 | 15-02-2024 | I4     |      20 |      2500 |
# |    5 | 01-03-2024 | I5     |      30 |      3000 |
# |    6 | 10-03-2024 | I6     |      40 |      3500 |
# |    7 | 20-03-2024 | I7     |      20 |      2500 |
# |    8 | 30-03-2024 | I8     |      10 |      1000 |

# | Year_Month | TotalSale | PercentageDiffPrevMonth |
# | ---------- | --------: | ----------------------: |
# | Jan-24     |      3000 |                    null |
# | Feb-24     |      4000 |                      25 |
# | Mar-24     |     10000 |                      60 |


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sales_data = [
    (1, "2024-01-01", "I1", 10, 1000),
    (2, "2024-01-15", "I2", 20, 2000),
    (3, "2024-02-01", "I3", 10, 1500),
    (4, "2024-02-15", "I4", 20, 2500),
    (5, "2024-03-01", "I5", 30, 3000),
    (6, "2024-03-10", "I6", 40, 3500),
    (7, "2024-03-20", "I7", 20, 2500),
    (8, "2024-03-30", "I8", 10, 1000)
]

sales_df = spark.createDataFrame(
    sales_data,
    ["SOID", "SODate", "ItemId", "ItemQty", "ItemValue"]
)

sales_df.show()

