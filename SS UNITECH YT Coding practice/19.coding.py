# Databricks notebook source
# input:
# | Vendor | Products    |
# | ------ | ----------- |
# | V1     | P1          |
# | V2     | P1,P2,P3,P4 |
# | V3     | P2,P3,P4    |

# output
# | Vendor | Products    | Product_Count |
# | ------ | ----------- | ------------- |
# | V1     | P1          | 1             |
# | V2     | P1,P2,P3,P4 | 4             |
# | V3     | P2,P3,P4    | 3             |



# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

vendor_data = [
    ("V1", "P1"),
    ("V2", "P1,P2,P3,P4"),
    ("V3", "P2,P3,P4")
]

vendor_df = spark.createDataFrame(
    vendor_data,
    ["Vendor", "Products"]
)

vendor_df.show(truncate=False)
