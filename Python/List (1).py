# Databricks notebook source
def lenarr(arr):
    # print(len(arr))
    return len(arr)

# COMMAND ----------

lenarr([54, 43, 2, 1, 5])

# COMMAND ----------

# Input: arr = [54, 43, 2, 1, 5]
# Output: 105
# Explanation: Just sum it 54 + 43 + 2 + 1 + 5 = 105.

# COMMAND ----------

sumarr = 0
arr = [54, 43, 2, 1, 5]
def listSum(arr):
  for i in arr:
    sumarr = sumarr + i
print(sumarr)

# COMMAND ----------

def add(a, b):
    return a + b

# COMMAND ----------

add(10,5)

# COMMAND ----------

arr = [54, 43, 2, 1, 5]
for i in arr:
    print(i-1,end=" ")

# COMMAND ----------

# Input: arr = [54, 43, 2, 1, 5]
# Output: 53 42 1 0 4
# Explanation: Just decrement the numbers by 1.

# COMMAND ----------

def add(a, b):
    return a + b

result = add(10, 5)
print(result)

# COMMAND ----------

def greet2(name):
    print("Hello", name)

    return2 name

# COMMAND ----------

def greet(name):
    print("Hello", name)

    return name

# greet("Sachin")

# COMMAND ----------

greet("Sachin")

# COMMAND ----------

sum = 0
for i in range (0,len(arr)):
    sum = sum + arr[i]
print(sum)      


# COMMAND ----------


