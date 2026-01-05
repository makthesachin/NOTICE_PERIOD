# Databricks notebook source
# | EmpId | EmpName | Skills         |
# | ----: | ------- | -------------- |
# |     1 | John    | ADF            |
# |     1 | John    | ADB            |
# |     1 | John    | PowerBI        |
# |     2 | Joanne  | ADF            |
# |     2 | Joanne  | SQL            |
# |     2 | Joanne  | Crystal Report |
# |     3 | Vikas   | ADF            |
# |     3 | Vikas   | SQL            |
# |     3 | Vikas   | SSIS           |
# |     4 | Monu    | SQL            |
# |     4 | Monu    | SSIS           |
# |     4 | Monu    | SSAS           |
# |     4 | Monu    | ADF            |


# | EmpName | Skills                 |
# | ------- | ---------------------- |
# | John    | ADF,ADB,PowerBI        |
# | Joanne  | ADF,SQL,Crystal Report |
# | Vikas   | ADF,SQL,SSIS           |
# | Monu    | SQL,SSIS,SSAS,ADF      |


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

emp_skill_data = [
    (1, "John", "ADF"),
    (1, "John", "ADB"),
    (1, "John", "PowerBI"),
    (2, "Joanne", "ADF"),
    (2, "Joanne", "SQL"),
    (2, "Joanne", "Crystal Report"),
    (3, "Vikas", "ADF"),
    (3, "Vikas", "SQL"),
    (3, "Vikas", "SSIS"),
    (4, "Monu", "SQL"),
    (4, "Monu", "SSIS"),
    (4, "Monu", "SSAS"),
    (4, "Monu", "ADF")
]

emp_skill_df = spark.createDataFrame(
    emp_skill_data,
    ["EmpId", "EmpName", "Skills"]
)

emp_skill_df.show(truncate=False)

