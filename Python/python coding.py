# Databricks notebook source
# DBTITLE 1,reverse
str1 = "abcd"
for i in range(len(str1)-1,-1,-1):
    print(str1[i],end="")

# COMMAND ----------

# DBTITLE 1,palindrome
str1 = 'MOOM'
rev = ''
for i in range(len(str1)-1,-1,-1):
    rev= rev+str1[i]
if str1 == rev:
        print('str1 is palindrome')
else:
        print('str1 is not palindrome')   

# COMMAND ----------

# DBTITLE 1,vowel and consonants
str1 = 'apple123'
vowels = 'aeiouAEIOU'
v_count = 0
c_count = 0
nvc_count = 0
for i in str1:
    if i.isalpha:
      if i in vowels:
        v_count =v_count + 1
        print('vowels are ',i)
      else:
        c_count = c_count + 1
        print('consonants are ',i)
    else:
        nvc_count = nvc_count + 1
        print('neither alpha , nor constant are',i)
        print('it is neither alpha nor constant')
# print(f'vowels are {v_count},constants are {c_count}')

# COMMAND ----------

str1 = 'aabbcccdddd abbccddd'
parts = str1.split(' ')
# print(parts)

for part in parts:
        result = { }
        for i in part:
            if i in result:
                result[i]+=1
            else:
                result[i]=1
        print(result)    

# COMMAND ----------

w = 'sachin is a good boy, sachin is a smart boy'
parts = w.split(' ')
print(parts)
for i in w:
    print(i,end='')


# COMMAND ----------

str1 = 'aabbcccdddd abbccddd'
parts = str1.split(' ')
freq_list = []
print(parts)

for part in parts:
    freq = {}
    for ch in part:
        freq[ch] = freq.get(ch, 0) + 1
    freq_list.append(freq)
    print(freq_list)

for idx, freq in enumerate(freq_list):
    print(f"Part {idx+1}: {freq}")

# COMMAND ----------

#count frequency of char in string
str1 = 'aabbcccdddd abbccddd'
empty = {}

for i in str1:
    if i in empty:
        empty[i] = empty[i]+1
    else:
        empty[i] = 1
print(empty)

# COMMAND ----------

# Count frequency of each character

s = "helol"

for i in range(len(s)):
    count = 0
    for j in range(len(s)):
        if s[i] == s[j]:
            count += 1
    print(s[i], count)


# COMMAND ----------

s = "programming"

freq = {}

for ch in s:
    if ch in freq:
        freq[ch] += 1
    else:
        freq[ch] = 1

print(freq)

# COMMAND ----------

# DBTITLE 1,Cell 6
freq = {'p': 1, 'r': 2, 'o': 1, 'g': 2, 'a': 1, 'm': 2, 'i': 1, 'n': 1}
freq['p']

# COMMAND ----------

freq = {'p': 1, 'r': 2, 'o': 1, 'g': 2, 'a': 1, 'm': 2, 'i': 1, 'n': 1}
for k, v in freq.items():
    print(k, v)

# COMMAND ----------

s = "programming"
result = {}
output = []
for i in s:    
    if i in result:
        result[i] = result[i]+1        
    else:
        result[i] = 1        
print(result)

# COMMAND ----------

s = "programming"

freq = {}
output = []

for ch in s:
    freq[ch] = freq.get(ch, 0) + 1
    output.append(f"{ch}{freq[ch]}")

print(" ".join(output))

# COMMAND ----------

s = "programming"

freq = {}
output = []

for ch in s:
    freq[ch] = freq.get(ch, 0) + 1

for ch in freq:
    output.append(f"{ch}{freq[ch]}")

print(" ".join(output))

# COMMAND ----------

#  5. Find length of string without using len()

str1 = 'aabbb'
count = 0
for i in str1:
    if i in str1:
        count = count + 1
print(count)


# COMMAND ----------

# 6. Remove spaces from a string

str1 = 'sachin makthe'
parts = str1.split(' ')
for i in parts:
    print(i,end = "")


# COMMAND ----------

# DBTITLE 1,Cell 17
# 7. Replace a character in a string
str1 = 'sachin nakthe'
str1 = str1.replace('n', 'm')
print(str1)

# COMMAND ----------

# 7. Replace a character in a string without using inbuilt method
str1 = 'sachin nakthe'
old_char = 'n'
new_char = 'm'
result = ''
for i in str1:
    if i == old_char:
        result = result + new_char       
    else:
        result = result + i
print(result)

# COMMAND ----------

# 7. Replace a character in a string without using inbuilt method
str1 = 'sachin nakthe'
old_char = 'z'
new_char = 'm'
result = ''
for ch in str1:
    if ch == old_char:
        result += new_char
    else:
        result += ch
print(result)

# COMMAND ----------

# 8. Convert lowercase to uppercase (without .upper())
str1 = 'sachinmakthe'
result = ''
for ch in str1:
    if 'a' <= ch <= 'z':
        result += chr(ord(ch) - 32)
    else:
        result += ch
print(result)

# COMMAND ----------

s = "python"
lower = "abcdefghijklmnopqrstuvwxyz"
upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

result = ""
for ch in s:
    for i in range(26):
        if ch == lower[i]:
            result += upper[i]
            break

print(result)

# COMMAND ----------

# DBTITLE 1,Untitled
s = 'sachin is good boy and sachin is smart boy'
freq = {}

# Count frequency
for ch in s:
    if ch in freq:
        freq[ch] += 1
    else:
        freq[ch] = 1

# Print duplicates
for ch in freq:
    if freq[ch] > 1:
        print(ch, freq[ch])

# COMMAND ----------

s = "programming"
n = len(s)

for i in range(n):
    count = 1
    if s[i] == "0":
        continue

    for j in range(i+1, n):
        if s[i] == s[j]:
            count += 1
            # mark as visited
            s = s[:j] + "0" + s[j+1:]

    if count > 1:
        print(s[i], count)
