# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

student_score_data = [
    ("Alice", "Math", 90),
    ("Alice", "Science", 85),
    ("Bob", "Math", 80),
    ("Bob", "Science", 75),
    ("Charlie", "Math", 95),
    ("Charlie", "Science", 90)
]

student_score_df = spark.createDataFrame(
    student_score_data,
    ["Name", "Subject", "Score"]
)

student_score_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum as sum_,col

dfpivot = student_score_df.groupby('Name').pivot('Subject').agg(sum_('Score'))
dfpivot.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Unpivot

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(Name="Alice", Math=90, Science=85),
    Row(Name="Bob", Math=80, Science=75),
    Row(Name="Charlie", Math=95, Science=90)
]

df = spark.createDataFrame(data)
display(df)

# COMMAND ----------


