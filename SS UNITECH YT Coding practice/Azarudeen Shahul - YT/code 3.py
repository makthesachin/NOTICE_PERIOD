# Databricks notebook source
# 3.Apply Line Break After Every 5th Pipe Delimiter
# Coding Question (Q15)

# You are given a single-row pipe (|) delimited text file.
# Each record consists of 5 fields:

# Name | Edu | YearOfExp | Tech | Mobnum
# The entire file is in one line.

# Task:
# Apply a line break after every 5th occurrence of | so that the data becomes multiple rows, and load it as a proper Spark DataFrame.

# Input raw Table 
# Azar|BE|8|BigData|9273564531|Ramesh|BTech|3|Java|8439761222|Parthiban|ME|6|dotNet|8876534121|Magesh|MCA|8|DBA|9023451789


# output table
# | Name      | Edu   | YearOfExp | Tech    | Mobnum     |
# | --------- | ----- | --------- | ------- | ---------- |
# | Azar      | BE    | 8         | BigData | 9273564531 |
# | Ramesh    | BTech | 3         | Java    | 8439761222 |
# | Parthiban | ME    | 6         | dotNet  | 8876534121 |
# | Magesh    | MCA   | 8         | DBA     | 9023451789 |

data = [
("Azar|BE|8|BigData|9273564531|Ramesh|BTech|3|Java|8439761222|Parthiban|ME|6|dotNet|8876534121|Magesh|MCA|8|DBA|9023451789",)
]

df_raw = spark.createDataFrame(data, ["raw_text"])
df_raw.show(truncate=False)

# COMMAND ----------

data = [ ("Name | Edu | YearOfExp | Tech | Mobnum",)]

df_raw_header = spark.createDataFrame(data, ["raw_text_header"])
from pyspark.sql.functions import split, col

df_raw_header = df_raw_header.withColumn('header', split(col('raw_text_header'), '\s*\|\s*'))
df_raw_header.show(truncate=False)

# COMMAND ----------

data = [ ("Name | Edu | YearOfExp | Tech | Mobnum",)]

df_raw_header = spark.createDataFrame(data, ["raw_text_header"])
from pyspark.sql.functions import split, col

df_raw_header = df_raw_header.withColumn('header', split(col('raw_text_header'), '\\|'))
df_raw_header.show(truncate=False)

# COMMAND ----------

data = [ ("Name | Edu | YearOfExp | Tech | Mobnum",)]

df_raw_header = spark.createDataFrame(data, ["raw_text_header"])
from pyspark.sql.functions import split, col

df_raw_header = df_raw_header.withColumn('header', split(col('raw_text_header'), '\s*\|\s*'))
df_raw_header = df_raw_header.select(
    col('header').getItem(0).alias('Name'),
    col('header').getItem(1).alias('Edu'),
    col('header').getItem(2).alias('YearOfExp'),
    col('header').getItem(3).alias('Tech'),
    col('header').getItem(4).alias('Mobnum')
)
display(df_raw_header)

# COMMAND ----------

from pyspark.sql.functions import split, posexplode, col, expr

# Split the raw text into fields
df_split = df_raw.withColumn("fields", split(col("raw_text"), "\\|"))

# Explode with position, group every 5 fields into a row
df_grouped = df_split.select(
    posexplode(col("fields")).alias("pos", "value")
).withColumn(
    "row_num", expr("int(pos/5)")
).groupBy("row_num").agg(
    expr("collect_list(value) as fields")
).select(
    col("fields").getItem(0).alias("Name"),
    col("fields").getItem(1).alias("Edu"),
    col("fields").getItem(2).alias("YearOfExp"),
    col("fields").getItem(3).alias("Tech"),
    col("fields").getItem(4).alias("Mobnum")
)

display(df_grouped)
