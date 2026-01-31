# Databricks notebook source
# MAGIC %md 
# MAGIC ![image_1769845539255.png](./image_1769845539255.png "image_1769845539255.png")

# COMMAND ----------

# Q7 – Which one returns result faster?

# You are given 4 Spark pipelines:

# df = spark.read.csv("dummy")
# df1 = df.filter("x")
# df2 = df1.sort("y")
# df2.count()

# df = spark.read.csv("dummy")
# df1 = df.sort("y")
# df2 = df1.filter("x")
# df2.count()

# df = spark.read.csv("dummy")
# df.cache()
# df1 = df.filter("x")
# df2 = df1.sort("y")
# df2.count()

# df = spark.read.csv("dummy")
# df.cache()
# df1 = df.sort("y")
# df2 = df1.filter("x")
# df2.count()





# COMMAND ----------

from pyspark.sql import Row

# Create DataFrames for table A and table B
df_A = spark.createDataFrame([Row(value=1), Row(value=2), Row(value=3)])
df_B = spark.createDataFrame([Row(value=1), Row(value=2), Row(value=4), Row(value=5), Row(value=6)])

# Display DataFrames
display(df_A)
display(df_B)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Left join df_A and df_B on value
import pyspark.sql.functions as F


a = df_A.alias('a')
b = df_B.alias('b')
left = a.join(b, F.col('a.value') == F.col('b.value'), 'left')
left.show()

# COMMAND ----------

# DBTITLE 1,Filter non-null values after join
from pyspark.sql.functions import col
# Use DataFrame indexing to avoid ambiguity
# This will select the first 'value' column (from df_A)
display(left.filter(F.col('b.value').isNull()))

# COMMAND ----------

# DBTITLE 1,Fix IndentationError in string counter code
str1 = 'abcd'
result = ""
prev = ""
count = 0
for i in str1:
    if i == prev:
        count += 1
    else:
        if prev:
            result += prev + str(count)
        prev = i
        count = 1
result += prev + str(count)
print(result)
