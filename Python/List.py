# Databricks notebook source
# DBTITLE 1,print even,odd from list
list = [1,2,3,4,5,6]
even_list = []
odd_list = []
for i in list:
    if i%2==0:
        even_list.append(i)
    else:
        odd_list.append(i)
print(even_list)
print(odd_list)

# COMMAND ----------

# DBTITLE 1,remove duplicates from list
list = [1,2,3,3,4,5]
remove_list = []
for i in list:
    if i  not in remove_list:
        remove_list.append(i)
        
    else:
        pass
print(remove_list)

# COMMAND ----------

# DBTITLE 1,list comprehention

