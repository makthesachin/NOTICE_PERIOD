# Databricks notebook source
# Dict is collection key value pairs
# dict_name = {key1:value1,key2:value2,....etc}
car = {'brand1':'Audi','model':'q7','brand2':'Bar'}
for i in car:
    print(i,car[i])
# print(car)
# len(car)

# COMMAND ----------

lst1 = ['sachin','makthe','maruthi','bamane']
lst2 = ['name','surname','nav','annav']
dict1 = dict(zip(lst2, lst1))
print(dict1)

# COMMAND ----------

print(car.keys())

# COMMAND ----------

if 'brand3' in car.keys():
    print('brand3 is present')
else:
    print('brand3 is not present')

# COMMAND ----------

if 'Audi'  in car.values():
    print('Audi  is present ')
else:
    print('Audi  is not present')

# COMMAND ----------

# dict constructor
var = dict(brand='Audi',model='q7')
# print(var)
var['brand']
var.get('brand')
