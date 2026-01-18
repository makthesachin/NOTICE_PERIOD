# Databricks notebook source
# DBTITLE 1,reverse
lst = [1,2,3,4,5]
rev = []
for i in range(len(lst)-1,-1,-1):
    rev.append(lst[i])
print(rev)

# COMMAND ----------

# DBTITLE 1,len list
lst = [1,2,3,4,5]
counter = 0
for i in lst:
    if i in lst:
        counter = counter + 1
print(counter)

# COMMAND ----------

# DBTITLE 1,check list empty or not
# lst = [1,2,3]
lst = []
if len(lst)==0:
    print("lst is empty")
else:
    print("lst is not empty")

# COMMAND ----------

lst = [1,11,2,3,4,5]

new = lst.sort()
print(lst)
print(new)

# COMMAND ----------

a = [1,2,3]
b = a
b.append(4)
print(a)
print(b)

# COMMAND ----------

a = [1,2,3]
b = a.copy()

b.append(4)

print(a)
print(b)

# COMMAND ----------

lst = [1,11,2,3,4,5]
new = sorted(lst)
print(new)

# COMMAND ----------

# Count the number of elements in a list

# COMMAND ----------

lst = [1, 2, 3, 5]
result = {}
for i in range (0,len(lst)-1):
    print(i)
   

# COMMAND ----------

lst = [1, 2, 3, 5]
result = {}
for i in range (0,len(lst)):
    print(lst[i])

# COMMAND ----------

lst = [1, 2, 3, 5]
result = {}
for i in range (0,len(lst)):
    print(lst[i])

# COMMAND ----------

lst = [11, 12, 13, 15]
result = {}
for i in range (min(lst),max(lst)-1):
    print(lst[i],end=",")
    print(i,end=",")

# COMMAND ----------

# input- lst = [1, 2, 3, 5]
# output - result = {4: 0}

lst = [11, 12, 13, 15]
result = {}
for i in range (min(lst),max(lst)+1):
    if i not in lst:
        result[i]=0
print(result)

# lst = [1, 2, 3, 5]
# result = {}
# for i in range(min(lst), max(lst) + 1):
#     if i not in lst:
#         result[i] = 0
# print(result)

# COMMAND ----------

lst = [1, 2, 3, 5]
result = {}

# output = {4,0}
for i  in result:
    if i not in result:
        result[i] = result[i]+0
    else:
        result[i]=1
print(result)

# COMMAND ----------

lst = [1, 2, 3, 5]
result = {}
for i in lst:
    if i in result:
        result[i] = result[i] +1
    else:
        result[i] = 1
print(result)

# COMMAND ----------

# DBTITLE 1,frequency in list
lst = [1, 2, 2, 3, 3, 3]
# result = {1: 1, 2: 2, 3: 3}
replst=[]
counter = 1
result = {}
for i in lst:
    replst.append(i)
    if i not in replst:
        result[i] = result[i]+counter
    else:
        counter = counter+1
        result[i]=result[i]+counter
print(result)

# COMMAND ----------


