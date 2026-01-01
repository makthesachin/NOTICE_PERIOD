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
list = [1,2,3,4,5]
sqr_list = []
for i in list:
    sqr_list.append(i*i)
print(sqr_list)

# COMMAND ----------

# DBTITLE 1,list comprehenstion applied
list = [1,2,3,4,5]
[i*i for i in list]

# COMMAND ----------

# DBTITLE 1,even check tradition
list = [1,2,3,4,5]
even_list = []
for i in list:
    if i % 2 == 0:
        even_list.append(i)
print(even_list)        

# COMMAND ----------

# DBTITLE 1,even list -listcomprehenstion
list = [1,2,3,4,5]
[ i for i in list if i%2==0]

# COMMAND ----------

list = ['hello','world']
upperlist = []
for i in list:
    upperlist.append(i.upper())
print(upperlist) 

# COMMAND ----------

list = ['hello','world']
[i.upper() for i in list]

# COMMAND ----------


