# Databricks notebook source
data = [
    ("Team1", "W"),
    ("Team1", "L"),
    ("Team1", "W"),
    ("Team2", "L"),
    ("Team2", "L"),
    ("Team2", "W"),
    ("Team3", "W"),
    ("Team3", "W"),
    ("Team3", "L"),
    ("Team3", "W")
]

columns = ["team_name", "result"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC select * , sum(result) over(parition by team_name) from df

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, sum(case when result = 'W' then 1 else 0 end) over(order b) as win_count from df

# COMMAND ----------

# MAGIC %sql
# MAGIC select team_name,
# MAGIC sum(case when result = 'W' then 1 else 0 end ) as wins,
# MAGIC sum(case when result = 'L' then 1 else 0 end) as loose
# MAGIC from df group by team_name

# COMMAND ----------

result_df = spark.sql("""
    SELECT
        team_name,
        SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses
    FROM df
    GROUP BY team_name
""")
display(result_df)

# COMMAND ----------


