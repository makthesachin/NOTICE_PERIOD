# Databricks notebook source
 a = '12321'
 b = ''
 for i in a:
     if i not in b:
         b = b + i
print(b)

# COMMAND ----------

a = '1234'
for i in range(0,len(a)):
    print(a[i])

# COMMAND ----------

 a = 'poop'
 b = ''
 for i in range(len(a)-1,-1,-1):
     b = b + a[i]     
print(b)

if a == b:
    print(f'a is palindrome number because a={a},b={b} are equal')
else:
    print(f'a is not palindrome number because a={a},b={b} is not equal')
 


# COMMAND ----------

# DBTITLE 1,reverse character
 a = 'geeks code quiz practice code'
 b = ''
 for i in range(len(a)-1,-1,-1):
     b = b + a[i]     
print(b)

# if a == b:
#     print(f'a is palindrome number because a={a},b={b} are equal')
# else:
#     print(f'a is not palindrome number because a={a},b={b} is not equal')
 


# COMMAND ----------

# DBTITLE 1,reverse words
a = 'sachin rohti virat dhoni'
a = a.split(" ")
print(a)
for i in range(len(a)-1,-1,-1):
    print(a[i],end=' ')

# COMMAND ----------

a = 'sachin rohti virat dhoni'
words = a.split()
print(words)
print(type(words))
print(' '.join(words[::-1]))

# COMMAND ----------

text = "one-two-three-four"
print(text.split("-", 2))

# COMMAND ----------

text = "one-two-three-four"
print(text.rsplit("-", 2))

# COMMAND ----------

items = ['apple', 'banana', 'orange']
result = ",".join(items)
print(result)

# COMMAND ----------

str = 'virat,rohit,dhoni,sachin'
splitVar = str.split(",")
print(splitVar)
for i in range(len(splitVar)-1,-1,-1):
    print(i,)

# COMMAND ----------

str = 'abbcccddd'
ext = ''
counter = 1
for i in str:
    ext = ext + i
    if i in ext:
        counter = counter + 1
        ext = ext + i + str(counter)
print(ext)

# COMMAND ----------

input_str = 'abbcccddd'
ext = ''
counter = 1
for i in input_str:
    ext = ext + i
    if i in ext:
        counter = counter + 1
        ext = ext + i + str(counter)
print(ext)

# COMMAND ----------

input_str = 'abbcccddd'
ext = ''
counter = 1

for i in range(1, len(input_str)):
    if input_str[i] == input_str[i-1]:
        counter += 1
    else:
        ext += input_str[i-1] + str(counter)
        counter = 1

ext += input_str[-1] + str(counter)
print(ext)


# COMMAND ----------

input_str = 'abbcccddd'
ext = ''
counter = 1

for i in range(1, len(input_str)):
    if input_str[i] == input_str[i-1]:
        counter += 1
    else:
        ext += input_str[i-1] + str(counter)
        counter = 1

ext += input_str[-1] + str(counter)
print(ext)

# COMMAND ----------

str = 'sachin is god of cricket and sachin is master-blaster'
exist = ''
counter = 0
for i in str:
    exist = exist + i
    if i in exist:
        counter+=counter
print(exist,counter,end = "")

# COMMAND ----------

# DBTITLE 1,word frequency in string
list = "'sachin' 'is' 'god' 'of' 'cricket' 'sachin' 'is' 'master-blaster'"
type(list)
counter = ""
for i in list:
    if i in counter:
        counter = counter+1
    print(i,counter)
