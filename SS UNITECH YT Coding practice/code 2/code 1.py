# Databricks notebook source
# Input 1:
# | Employee_Id | Employee_Name |
# | ----------: | ------------- |
# |           1 | Niranjan      |
# |           2 | Rakesh        |
# |           3 | Vignesh       |

# Input 2:
# | Employee_Id | Certification_Name | Score |
# | ----------: | ------------------ | ----: |
# |           1 | Databricks         |   783 |
# |           2 | Pyspark            |   850 |
# |           3 | SQL                |   900 |
# |           2 | SQL                |   950 |
# |           1 | Pyspark            |   400 |
# |           2 | Databricks         |   590 |
# |           3 | Databricks         |   700 |
# |           1 | SQL                |   890 |
# |           3 | Pyspark            |   800 |

# output
# | Employee_Id | Employee_Name | Percentage | Result       |
# | ----------: | ------------- | ---------: | ------------ |
# |           1 | Niranjan      |       69.1 | Second Class |
# |           2 | Rakesh        |       79.6 | First Class  |
# |           3 | Vignesh       |       82.6 | Distinction  |

employee_data = [
    (1, "Niranjan"),
    (2, "Rakesh"),
    (3, "Vignesh")
]

employee_df = spark.createDataFrame(
    employee_data,
    ["Employee_Id", "Employee_Name"]
)

employee_df.show()

score_data = [
    (1, "Databricks", 783),
    (2, "Pyspark", 850),
    (3, "SQL", 900),
    (2, "SQL", 950),
    (1, "Pyspark", 400),
    (2, "Databricks", 590),
    (3, "Databricks", 700),
    (1, "SQL", 890),
    (3, "Pyspark", 800)
]

score_df = spark.createDataFrame(
    score_data,
    ["Employee_Id", "Certification_Name", "Score"]
)

score_df.show()
