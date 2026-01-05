# Databricks notebook source
# input:
# | actor_id | director_id | timestamp |
# | -------: | ----------: | --------: |
# |        1 |           1 |         0 |
# |        1 |           1 |         1 |
# |        1 |           1 |         2 |
# |        1 |           2 |         3 |
# |        1 |           2 |         4 |
# |        2 |           1 |         5 |
# |        2 |           1 |         6 |

# output:
# | actor_id | director_id |
# | -------: | ----------: |
# |        1 |           1 |



# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

actor_director_data = [
    (1, 1, 0),
    (1, 1, 1),
    (1, 1, 2),
    (1, 2, 3),
    (1, 2, 4),
    (2, 1, 5),
    (2, 1, 6)
]

actor_director_df = spark.createDataFrame(
    actor_director_data,
    ["actor_id", "director_id", "timestamp"]
)

actor_director_df.show()

