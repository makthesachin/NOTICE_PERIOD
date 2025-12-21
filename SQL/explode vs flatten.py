# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

data = [
    (1, [["spark", "sql"], ["delta", "lake"]]),
    (2, [["python"], ["pyspark"]]),
    (3, []),
    (4, None)
]

schema = StructType([
    StructField("id", IntegerType()),
    StructField("tags", ArrayType(ArrayType(StringType())))
])

tags_df = spark.createDataFrame(data, schema)
tags_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *

dfflat2 = tags_df.select('*',flatten(col('tags')).alias('merged_tags')).drop(col('tags'))
dfflat2.show(truncate=False)
dfflat2explode = dfflat2.select('*',explode('merged_tags'))
display(dfflat2explode)

# COMMAND ----------

tags_df.createOrReplaceTempView('tags')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, tag
# MAGIC FROM tags
# MAGIC LATERAL VIEW EXPLODE(FLATTEN(tags)) t AS tag;
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
tags_df.select("id",F.explode_outer(F.flatten("tags")).alias("tag")).show()

# COMMAND ----------


