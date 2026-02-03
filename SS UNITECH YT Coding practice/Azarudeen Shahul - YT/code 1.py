# Databricks notebook source
# Load File with Custom Delimiter (~|) into Spark DataFrame
# Coding Question

# Q13.
# You are given a text file where the delimiter is ~|.
# The file also contains:
# A header
# Comma inside the Name field
# Write PySpark code (using SparkSession, not SparkContext) to load this file into a Spark DataFrame with proper columns.

# Input:
# Name~|Age
# Azarudeen, Shahul~|25
# Michel, Clarke~|26
# Virat, Kohli~|28
# Andrew, Simond~|37
# Geogre, Bush~|59
# Flintoff, David~|12
# Adam, James~|20

# output:
# | Name              | Age |
# | ----------------- | --- |
# | Azarudeen, Shahul | 25  |
# | Michel, Clarke    | 26  |
# | Virat, Kohli      | 28  |
# | Andrew, Simond    | 37  |
# | Geogre, Bush      | 59  |
# | Flintoff, David   | 12  |
# | Adam, James       | 20  |



# COMMAND ----------

raw_data = [
    ("Name~|Age",),
    ("Azarudeen, Shahul~|25",),
    ("Michel, Clarke~|26",),
    ("Virat, Kohli~|28",),
    ("Andrew, Simond~|37",),
    ("Geogre, Bush~|59",),
    ("Flintoff, David~|12",),
    ("Adam, James~|20",)
]

raw_df = spark.createDataFrame(raw_data, ["raw_text"])
raw_df.show(truncate=False)


# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

df = raw_df.withColumn('name',split(df.raw_text,"~|"))
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split

df = raw_df.withColumn('name', split(raw_df.raw_text, "~\\|")[0])\
.withColumn('age',split(raw_df.raw_text,"~\\|")[1])
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.functions import explode

df = raw_df.withColumn('name', split(raw_df.raw_text, "~\\|")[0])\
.withColumn('age',split(raw_df.raw_text,"~\\|")[1])
df2 = df.withColumn('first_name',explode(split(df.name,",")))
df2.drop('raw_text','Name').show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split, col

df_final = (
    raw_df
    .filter(col("raw_text") != "Name~|Age")   # remove header
    .withColumn("Name", split(col("raw_text"), "~\\|")[0])
    .withColumn("Age", split(col("raw_text"), "~\\|")[1])
    .drop("raw_text")
)

df_final.show(truncate=False)
df_final.printSchema()

# COMMAND ----------


