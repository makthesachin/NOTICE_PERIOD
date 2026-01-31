# Databricks notebook source
# Input 1:
# | Employee_Id | Employee_Name |
# | ----------: | ------------- |
# |           1 | Niranjan      |
# |           2 | Rakesh        |
# |           3 | Vignesh       |

# Input 2:
# | Employee_Id | Certification_Name | Score |
# | ----------: | ------------------ | ----: |
# |           1 | Databricks         |   783 |
# |           2 | Pyspark            |   850 |
# |           3 | SQL                |   900 |
# |           2 | SQL                |   950 |
# |           1 | Pyspark            |   400 |
# |           2 | Databricks         |   590 |
# |           3 | Databricks         |   700 |
# |           1 | SQL                |   890 |
# |           3 | Pyspark            |   800 |

# output
# | Employee_Id | Employee_Name | Percentage | Result       |
# | ----------: | ------------- | ---------: | ------------ |
# |           1 | Niranjan      |       69.1 | Second Class |
# |           2 | Rakesh        |       79.6 | First Class  |
# |           3 | Vignesh       |       82.6 | Distinction  |

employee_data = [
    (1, "Niranjan"),
    (2, "Rakesh"),
    (3, "Vignesh")
]

employee_df = spark.createDataFrame(
    employee_data,
    ["Employee_Id", "Employee_Name"]
)

employee_df.show()

score_data = [
    (1, "Databricks", 783),
    (2, "Pyspark", 850),
    (3, "SQL", 900),
    (2, "SQL", 950),
    (1, "Pyspark", 400),
    (2, "Databricks", 590),
    (3, "Databricks", 700),
    (1, "SQL", 890),
    (3, "Pyspark", 800)
]

score_df = spark.createDataFrame(
    score_data,
    ["Employee_Id", "Certification_Name", "Score"]
)

score_df.show()

# COMMAND ----------

e = employee_df.alias("e")
s = score_df.alias('d')
df_join = e.join(s,e.Employee_Id == s.Employee_Id,'inner')
df_join = df_join.drop(s.Employee_Id)
df_join.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import col,sum,count,round
df_groupby = df_join.groupby('Employee_Id','Employee_Name').agg(round((sum('Score')/count(col('Employee_Id'))/1000*100),2).alias('sum_score'))
# df_groupby.show()
df_new = df_groupby.withColumn('Result',when(col('sum_score')>60 then 'sum_score'='second class'),
                                      when(col('sum_score')>70 then 'sum_score' = 'first class'),
                                      when(col('sum_score')>80 then 'sum_score' = 'distinction')
df_new.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, round, when

df_groupby = df_join.groupby('Employee_Id', 'Employee_Name').agg(
    round((sum('Score') / count(col('Employee_Id')) / 1000 * 100), 2).alias('sum_score')
)

df_new = df_groupby.withColumn(
    'Result',
    when(col('sum_score') >= 80, 'distinction')
    .when(col('sum_score') > 70, 'first class')
    .when(col('sum_score') > 60, 'second class')
    .otherwise('fail')
)
df_new.show()
