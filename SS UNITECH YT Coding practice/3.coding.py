# Databricks notebook source
# | student_id | student_name |
# | ---------- | ------------ |
# | 1          | Steve        |
# | 2          | David        |
# | 3          | Aryan        |


# | student_id | subject_name | marks |
# | ---------- | ------------ | ----- |
# | 1          | pyspark      | 90    |
# | 1          | sql          | 100   |
# | 2          | sql          | 70    |
# | 2          | pyspark      | 60    |
# | 3          | sql          | 30    |
# | 3          | pyspark      | 20    |


# | Percentage | Result       |
# | ---------- | ------------ |
# | ≥ 70       | Distinction  |
# | 60–69      | First Class  |
# | 50–59      | Second Class |
# | 40–49      | Third Class  |
# | ≤ 39       | Fail         |




# COMMAND ----------

# DBTITLE 1,student table
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('df').getOrCreate()

data = [(1,'steve'),(2,'David'),(3,'Aryan')]
schema =['student_id','subject_name']

studentdf = spark.createDataFrame(data,schema)
studentdf.show()

# COMMAND ----------

# DBTITLE 1,marks
marks_data = [
    (1, "pyspark", 90),
    (1, "sql", 100),
    (2, "sql", 70),
    (2, "pyspark", 60),
    (3, "sql", 30),
    (3, "pyspark", 20)
]

marksdf = spark.createDataFrame(
    marks_data,
    ["student_id", "subject_name", "marks"]
)

marksdf.show()

# COMMAND ----------

marksdf.createOrReplaceTempView('marksdf')
studentdf.createOrReplaceTempView('studentdf')

# COMMAND ----------

# MAGIC %sql
# MAGIC select student_id as marks_student_id from marksdf

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,marksdf.student_id as marks_student_id, studentdf.student_id as student_student_id
# MAGIC from marksdf
# MAGIC inner join studentdf
# MAGIC on marksdf.student_id == studentdf.student_id

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     marksdf.*, 
# MAGIC     studentdf.student_id AS studentdf_student_id
# MAGIC   FROM marksdf
# MAGIC   INNER JOIN studentdf
# MAGIC     ON marksdf.student_id = studentdf.student_id
# MAGIC ),
# MAGIC cte1 AS (
# MAGIC   SELECT 
# MAGIC     *, 
# MAGIC     AVG(marks) OVER(PARTITION BY student_id,subject_name) AS avg_marks
# MAGIC   FROM cte
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   *, 
# MAGIC   CASE 
# MAGIC     WHEN avg_marks >= 70 THEN 'Distinction'
# MAGIC     WHEN avg_marks BETWEEN 60 AND 69 THEN 'First Class'
# MAGIC     WHEN avg_marks BETWEEN 50 AND 59 THEN 'Second Class'
# MAGIC     WHEN avg_marks BETWEEN 40 AND 49 THEN 'Third Class'
# MAGIC     ELSE 'Fail'
# MAGIC   END AS result
# MAGIC FROM cte1

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC   SELECT 
# MAGIC     marksdf.*, 
# MAGIC     studentdf.student_id AS studentdf_student_id
# MAGIC   FROM marksdf
# MAGIC   INNER JOIN studentdf
# MAGIC     ON marksdf.student_id = studentdf.student_id
# MAGIC ),
# MAGIC cte1 as(
# MAGIC SELECT 
# MAGIC   *, 
# MAGIC   AVG(marks) OVER(PARTITION BY student_id) AS avg_marks
# MAGIC FROM cte)
# MAGIC
# MAGIC
# MAGIC select *, 
# MAGIC   case  
# MAGIC   when avg_marks >= 70 then 'Distinction'
# MAGIC   when avg_marks between 60 and 69 then 'First Class'
# MAGIC   when avg_marks between 50 and 59 then 'Second Class'
# MAGIC   when avg_marks between 40 and 49 then 'Third Class'
# MAGIC   else avg_marks < 39 then 'Fail'  
# MAGIC   end as result
# MAGIC from cte1

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT 
# MAGIC     marksdf.*, 
# MAGIC     studentdf.student_id AS studentdf_student_id
# MAGIC   FROM marksdf
# MAGIC   INNER JOIN studentdf
# MAGIC     ON marksdf.student_id = studentdf.student_id

# COMMAND ----------

# MAGIC %sql
# MAGIC  select *,marksdf.student_id as marksdf_student_id, studentdf.student_id as studentdf_student_id
# MAGIC from marksdf
# MAGIC inner join studentdf
# MAGIC on marksdf.student_id == studentdf.student_id

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import avg as _avg

joindf = studentdf.join(marksdf,studentdf.student_id==marksdf.student_id,'inner')
joindf1 = joindf.groupBy('student_id').agg(_avg(col('marks')))
display(joindf1)

# COMMAND ----------

# DBTITLE 1,u
studentdf.join(marksdf,studentdf.student_id==marksdf.student_id,'inner').show()

# COMMAND ----------


