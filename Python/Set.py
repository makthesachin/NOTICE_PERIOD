# Databricks notebook source
my_tuple = (1, 2, 3, 5, 1)
a = list(my_tuple)
a.append(8)
my_tuple = tuple(a)
print(my_tuple)

# COMMAND ----------

a = (1,2,3,4)
a = list(a)
a.append(1)
b = tuple(a)
b

# COMMAND ----------

my_tuple = (1,2,3,5,1)
a=list(my_tuple)
a.append(8)
print(type(a))
# my_tuple = tuple(a)
print(my_tuple)

# COMMAND ----------

list[1] + list(tuple)

# COMMAND ----------

list(tuple)
[1] + tuple
print(tuple)

# COMMAND ----------


