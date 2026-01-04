# Databricks notebook source
# Input:
# +-----+-----+-----+
# |City1|City2|City3|
# +-----+-----+-----+
# |Goa  |AP   |AP   |
# |null |AP   |null |
# |null |null |Bglr |
# +-----+-----+-----+

# output:
# +------+
# |Result|
# +------+
# |Goa   |
# |AP    |
# |Bglr  |
# +------+

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('df').getOrCreate()

data = [('Goa','','AP'),('','AP',None),(None,'','Bglr')]
schema = ['City1','City2','City3']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('df').getOrCreate()

data = [('Goa',None,'AP'),(None,'AP',None),(None,None,'Bglr')]
schema = ['City1','City2','City3']
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn('result',coalesce('city1','city2','city3')).show()

# COMMAND ----------

from pyspark.sql.functions import col, explode, array

result_df = (
    df
    .select(explode(array("City1", "City2", "City3")).alias("Result"))
    .filter(col("Result").isNotNull())
    .distinct()
)

result_df.show()


# COMMAND ----------

from pyspark.sql.functions import col, explode, array

result_df = (
    df
    .select(explode(array(["City1", "City2", "City3"])).alias('result'))
    .filter(col('result').isNotNull())
    .distinct()
   
)

result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Converts empty string "" → NULL
# MAGIC Keeps real values unchanged
# MAGIC Important because:
# MAGIC coalesce() does NOT treat empty string as null
# MAGIC So this conversion is necessary

# COMMAND ----------


