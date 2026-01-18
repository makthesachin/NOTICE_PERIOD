# Databricks notebook source
spark

# COMMAND ----------


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PracticeDF").getOrCreate()

data = [
    ("Alice", "Badminton, Tennis"),
    ("Bob", "Tennis, Cricket"),
    ("Julie", "Cricket, Carroms")
]

columns = ["name", "hobbies"]

df = spark.createDataFrame(data, columns)
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split,explode
df.withColumn('hobbies_',split('hobbies',",")).drop('hobbies').show()

# COMMAND ----------

from pyspark.sql.functions import split,explode
df.withColumn('hobbies_',explode(split('hobbies',","))).drop('hobbies').show()
df.printSchema()

# COMMAND ----------

txt = "hello, my name is Peter, I am 26 years old"

x = txt.split(", ")

print(x)

# COMMAND ----------

txt = "apple#banana#cherry#orange"

x = txt.split("#")

print(x)

# COMMAND ----------

txt = "apple#banana#cherry#orange"

# setting the maxsplit parameter to 1, will return a list with 2 elements!
x = txt.split("#",4)

print(x)

# COMMAND ----------

a = " Hello, World! "
print(a.strip()) # returns "Hello, World!"

# COMMAND ----------

a = "Hello, World!"
print(a[0])

# COMMAND ----------

txt = "The best things in life are free!"
print("free" in txt)

# COMMAND ----------

txt = "The best things in life are free!"
if '1' in txt:
    print('yes present')
else:
    print('not present')

# COMMAND ----------

txt = "I love apples, apple are my favorite fruit"
x = txt.count("apple")
print(x)

# COMMAND ----------

txt = "I love apples, apple are my favorite fruit"
x = txt.count("apple", 15, 24)
print(x)

# COMMAND ----------


