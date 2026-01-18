# Databricks notebook source
# DBTITLE 1,Basic Pivot
# input table:
# | month | amount |
# | ----- | ------ |
# | Jan   | 1000   |
# | Feb   | 1200   |
# | Mar   | 1500   |

# output table:
# | Jan  | Feb  | Mar  |
# | ---- | ---- | ---- |
# | 1000 | 1200 | 1500 |

from pyspark.sql import Row

sales_data = [
    Row(month="Jan", amount=1000),
    Row(month="Feb", amount=1200),
    Row(month="Mar", amount=1500)
]

sales_df = spark.createDataFrame(sales_data)
sales_df.printSchema()
sales_df.show()

# COMMAND ----------

# DBTITLE 1,Product-wise Monthly Sales
# Input:
# | product | month | amount |
# | ------- | ----- | ------ |
# | A       | Jan   | 100    |
# | A       | Feb   | 120    |
# | B       | Jan   | 90     |
# | B       | Feb   | 110    |

# output:
# | product | Jan | Feb |
# | ------- | --- | --- |
# | A       | 100 | 120 |
# | B       | 90  | 110 |

sales_data = [
    Row(product="A", month="Jan", amount=100),
    Row(product="A", month="Feb", amount=120),
    Row(product="B", month="Jan", amount=90),
    Row(product="B", month="Feb", amount=110)
]

sales_df = spark.createDataFrame(sales_data)
sales_df.printSchema()
sales_df.show()

# COMMAND ----------

# DBTITLE 1,Department Salary Pivot
# Input:
# | dept | salary |
# | ---- | ------ |
# | IT   | 50     |
# | IT   | 60     |
# | HR   | 40     |
# | HR   | 45     |

# output:
# | IT  | HR |
# | --- | -- |
# | 110 | 85 |

emp_data = [
    Row(dept="IT", salary=50),
    Row(dept="IT", salary=60),
    Row(dept="HR", salary=40),
    Row(dept="HR", salary=45)
]

emp_df = spark.createDataFrame(emp_data)
emp_df.printSchema()
emp_df.show()

# COMMAND ----------

# DBTITLE 1,Student Marks Pivot
# | student | subject | marks |
# | ------- | ------- | ----- |
# | John    | Math    | 90    |
# | John    | Science | 85    |
# | Alice   | Math    | 95    |
# | Alice   | Science | 90    |

# | student | Math | Science |
# | ------- | ---- | ------- |
# | John    | 90   | 85      |
# | Alice   | 95   | 90      |

marks_data = [
    Row(student="John", subject="Math", marks=90),
    Row(student="John", subject="Science", marks=85),
    Row(student="Alice", subject="Math", marks=95),
    Row(student="Alice", subject="Science", marks=90)
]

marks_df = spark.createDataFrame(marks_data)
marks_df.printSchema()
marks_df.show()

# COMMAND ----------

# DBTITLE 1,Attendance Pivot (Missing Values)
# | emp | day | status |
# | --- | --- | ------ |
# | E1  | Mon | P      |
# | E1  | Tue | P      |
# | E2  | Mon | P      |

# | emp | Mon | Tue  |
# | --- | --- | ---- |
# | E1  | P   | P    |
# | E2  | P   | NULL |

attendance_data = [
    Row(emp="E1", day="Mon", status="P"),
    Row(emp="E1", day="Tue", status="P"),
    Row(emp="E2", day="Mon", status="P")
]

attendance_df = spark.createDataFrame(attendance_data)
attendance_df.printSchema()
attendance_df.show()

# COMMAND ----------

# DBTITLE 1,Year & Month Pivot (Very Common Interview)
# Input:
# | year | month | amount |
# | ---- | ----- | ------ |
# | 2023 | Jan   | 100    |
# | 2023 | Feb   | 120    |
# | 2024 | Jan   | 150    |
# | 2024 | Feb   | 170    |

# output
# | year | Jan | Feb |
# | ---- | --- | --- |
# | 2023 | 100 | 120 |
# | 2024 | 150 | 170 |

revenue_data = [
    Row(year=2023, month="Jan", amount=100),
    Row(year=2023, month="Feb", amount=120),
    Row(year=2024, month="Jan", amount=150),
    Row(year=2024, month="Feb", amount=170)
]

revenue_df = spark.createDataFrame(revenue_data)
revenue_df.printSchema()
revenue_df.show()

# COMMAND ----------

# DBTITLE 1,Pivot WITHOUT pivot() (CASE style)
# | month | amount |
# | ----- | ------ |
# | Jan   | 100    |
# | Feb   | 120    |
# | Mar   | 150    |

# | Jan | Feb | Mar |
# | --- | --- | --- |
# | 100 | 120 | 150 |

from pyspark.sql import Row

sales_data = [
    Row(month="Jan", amount=100),
    Row(month="Feb", amount=120),
    Row(month="Mar", amount=150)
]

sales_df = spark.createDataFrame(sales_data)
sales_df.printSchema()
sales_df.show()
