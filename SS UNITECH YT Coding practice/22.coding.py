# Databricks notebook source
# input tables:
# | id |
# | -- |
# | 1  |
# | 2  |
# | 3  |

# | id |
# | -- |
# | 2  |
# | 3  |
# | 4  |

# output
# Expected Output (Reference)
# Inner Join → 2, 3
# Left Join → 1, 2, 3
# Right Join → 2, 3, 4
# Full Join → 1, 2, 3, 4

# COMMAND ----------

table_a_data = [(1,), (2,), (3,)]
table_b_data = [(2,), (3,), (4,)]

table_a_df = spark.createDataFrame(table_a_data, ["id"])
table_b_df = spark.createDataFrame(table_b_data, ["id"])

table_a_df.show()
table_b_df.show()

# COMMAND ----------


# table_a_df , table_b_df
inner = table_a_df.join(table_b_df,table_a_df.id == table_b_df.id,'inner')
inner.show()

# COMMAND ----------

left=table_a_df.join(table_b_df,table_a_df.id==table_b_df.id,'left')
left.show()

# COMMAND ----------

union = table_a_df.union(table_b_df)
union.show()
