# Databricks notebook source
# input:
# | Value |
# | ----- |
# | A1    |
# | A2    |
# | C1    |
# | C2    |

# output:
# | C1 | C2 |
# | -- | -- |
# | A1 | C1 |
# | A2 | C2 |

# COMMAND ----------

value_data = [
    ("A1",),
    ("A2",),
    ("C1",),
    ("C2",)
]

value_df = spark.createDataFrame(
    value_data,
    ["Value"]
)

value_df.show()
