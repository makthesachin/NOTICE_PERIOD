# Databricks notebook source


# COMMAND ----------

# Write a PySpark program to generate a report that returns all pairs of (actor_id, director_id) where the actor has worked (cooperated) with the same director at least 3 times.

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

# COMMAND ----------

1705562400-

# COMMAND ----------

# DBTITLE 1,Cell 4
from pyspark.sql.functions import col

df = actor_director_df.groupby('actor_id','director_id').count()
df1 = df.filter(col('count') > 2).drop('count')
df1.show()
