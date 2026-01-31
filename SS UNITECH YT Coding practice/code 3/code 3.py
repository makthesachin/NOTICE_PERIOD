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

# input table 
# | Name      | Edu   | YearOfExp | Tech    | Mobnum     |
# | --------- | ----- | --------- | ------- | ---------- |
# | Azar      | BE    | 8         | BigData | 9273564531 |
# | Ramesh    | BTech | 3         | Java    | 8439761222 |
# | Parthiban | ME    | 6         | dotNet  | 8876534121 |
# | Magesh    | MCA   | 8         | DBA     | 9023451789 |

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

# COMMAND ----------

# DBTITLE 1,Cell 2
df = spark.createDataFrame([(x,) for x in range(10)], ['value'])

# COMMAND ----------


