# Databricks notebook source
a = 'hi'
b = 'there'
# op = 'hitherehi'

print(a+b+a)

# COMMAND ----------

str = 'hello' 
x = 'llo'
print(str.title())
print(str.find(x))
print(str.strip())
print(str.swapcase())

# COMMAND ----------

# DBTITLE 1,upper case
s = "ABCddE"
op="abcdde"
print(s.upper())

# COMMAND ----------

# DBTITLE 1,reverse string
s = "dlroW"
r = ""
for i in range(len(s)-1,-1,-1):
    r = r + s[i]
print(r)


# COMMAND ----------

# DBTITLE 1,palindrome check
s = "Hello"
r = ""
for i in range(len(s)-1,-1,-1):
    r = r+s[i]
if r.lower() == s.lower():
    print(f'{s} is a palindrome because {s},{r} are equal')
else:
    print(f'{s} is not a palindrome because {s},{r} are not equal')

# COMMAND ----------

# DBTITLE 1,de

