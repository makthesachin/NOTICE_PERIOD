# Databricks notebook source

names = ['john', 'ala', 'ilia', 'sudan', 'mercy'] 
marks = [100, 200, 150, 80, 300]

result = {}
for i in range(len(names)):
    result[names[i]] = marks[i]
print(result)

# COMMAND ----------

names = ['john', 'ala', 'ilia', 'sudan', 'mercy'] 
marks = [100, 200, 150, 80, 300]
dict ={}
for i in range (0,len(names)):
    dict[names[i]] = marks[i]
print(dict)
# dict = dict(zip(names, marks))

# COMMAND ----------

names = ['john', 'ala', 'ilia', 'sudan', 'mercy'] 
marks = [100, 200, 150, 80, 300]
dict = {name: mark for name, mark in zip(names, marks)}

# COMMAND ----------

student = {"name": "Sachin", "age": 25, "course": "Python"}
print(student)

# COMMAND ----------

print(student["name"])        # Sachin
print(student.get("age"))     # 25

# COMMAND ----------

student
student["city"] = "Hyderabad"   # add
student["age"] = 26             # update
print(student)

# COMMAND ----------

student['age'] = 26
# student['city']='hyderabad'

# COMMAND ----------

student.pop("course")
print(student)

# COMMAND ----------


