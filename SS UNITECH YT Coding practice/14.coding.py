# Databricks notebook source
# DBTITLE 1,max list
lst = [11, 22, 3, 4, 100]
max_val = lst[0]   # initialize with first element
for i in lst:
    if i > max_val:
        max_val = i
print(max_val)

# COMMAND ----------

# DBTITLE 1,min list
lst = [2,1,3,6,9]
min = lst[0]
for i in lst:
    if i < min:
        min = i
print(min)

# COMMAND ----------

# DBTITLE 1,reverse
lst = [2,1,3,6,9]
reverse =[]
for i in range(len(lst)-1,-1,-1):
    reverse.append(lst[i])
print(reverse,end="")

# COMMAND ----------

lst = [2,1,3,6,9]
n = len(lst)
for i in lst:
    for j in range(0,len(lst)-i-1):
        if lst[j]>lst[j+1]:
            lst[j],lst[j+1] = lst[j+1],lst[j]
print(lst)

# COMMAND ----------

lst = [3,4,1,2,7,4,5,9,8]
for i in lst:
    for j in range(0,len(lst)-i-1):
        if lst[j]>lst[j+1]:
            lst[j],lst[j+1] = lst[j+1],lst[j]
print(lst)

# COMMAND ----------

lst = [3,4,1,2,7,4,5,9,8]
for i in lst:
    for j in range(0,len(lst-i)-1):
        if lst[j]>lst[j+1]:
            lst[j],lst[j+1] = lst[j+1],lst[j]
print(lst)

# COMMAND ----------

mlst = ['a', 'b', 'c', 'd', 'c']
result = []
count_dict = {}

for i in lst:
    count_dict[i] = count_dict.get(i, 0) + 1
    if i == 'd':
        result.append(i)
    else:
        result.append(f"{i}{count_dict[i]}")
print(result)

# COMMAND ----------

lst = ['a','b', 'c', 'd', 'c']
string = ""
counter =1
for i in lst:
    string = string + i
    if i not in string:
        counter = 1
        string = string + i + counter
    else:
        counter = counter + 1
        string = string + i + str(counter)
string

# COMMAND ----------

lst = ['a', 'b', 'c', 'd', 'c']
lstcnt=[]
counter = 1
for i in range(0,len(lst)-1):
    if lst[i]==lst[i+1]:
        counter = counter + 1
        lstcnt.append(lst[i])
    else:
        lstcnt
print(lstcnt)  

# COMMAND ----------

# 1.520 -0.712
0.808/2

# COMMAND ----------

# | Machine_id | process_id | activity_type | timestamp |
# | ---------: | ---------: | ------------- | --------: |
# |          0 |          0 | start         |     0.712 |
# |          0 |          0 | end           |     1.520 |
# |          0 |          1 | start         |     3.140 |
# |          0 |          1 | end           |     4.120 |
# |          1 |          0 | start         |     0.550 |
# |          1 |          0 | end           |     1.550 |
# |          1 |          1 | start         |     0.430 |
# |          1 |          1 | end           |     1.420 |
# |          2 |          0 | start         |     4.100 |
# |          2 |          0 | end           |     4.512 |
# |          2 |          1 | start         |     2.500 |
# |          2 |          1 | end           |     5.000 |

# | Machine_id | avg_processing_time |
# | ---------: | ------------------: |
# |          0 |               0.894 |
# |          1 |               0.995 |
# |          2 |               1.456 |



# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

machine_data = [
    (0, 0, "start", 0.712),
    (0, 0, "end",   1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end",   4.120),

    (1, 0, "start", 0.550),
    (1, 0, "end",   1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end",   1.420),

    (2, 0, "start", 4.100),
    (2, 0, "end",   4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end",   5.000)
]

machine_df = spark.createDataFrame(
    machine_data,
    ["machine_id", "process_id", "activity_type", "timestamp"]
)

machine_df.show()

# COMMAND ----------

df1 = machine_df.groupby('machine_id').sum('timestamp').alias('sum_timestamp')
display(df1)

# COMMAND ----------

dfgroup = machine_df.groupby('machine_id', 'activity_type').sum('timestamp').withColumnRenamed('sum(timestamp)', 'sum_start_timestamp')
display(dfgroup)

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

dfgroup = machine_df.groupBy('Machine_id', 'activity_type').agg(_sum('timestamp').alias('sum_start_timestamp'))
dfgroup.show()

# COMMAND ----------

from pyspark.sql.functions import when, col,count,round

df_updated = dfgroup.withColumn("sum_start_timestamp",when(col("activity_type") == "start",-col("sum_start_timestamp"))\
                            .otherwise(col("sum_start_timestamp")))

df_updated.show()

df_final = df_updated.groupby('Machine_id').agg(round(_sum('sum_start_timestamp')/count('sum_start_timestamp'),3).alias('avg_processing_time'))
df_final.show()


# COMMAND ----------

df_final = df_updated.groupby('Machine_id').agg(round(_sum('sum_start_timestamp')/count('sum_start_timestamp'),3).alias('avg_processing_time'))
df_final.show()
