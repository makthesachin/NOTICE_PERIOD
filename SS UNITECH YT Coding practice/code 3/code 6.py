# Databricks notebook source
# Q6 – Problem Statement

# From the given CSV file, calculate Expiry_date by adding
# Remaining_days to Rechargedate.

# Input table
# | RechargeId | Rechargedate | Remaining_days | validity |
# | ---------- | ------------ | -------------- | -------- |
# | R201623    | 20200511     | 1              | online   |
# | R201873    | 20200119     | 110            | online   |
# | R201999    | 20200105     | 35             | online   |
# | R201951    | 20191105     | 215            | online   |

# Expected table
# | RechargeId | Rechargedate | Remaining_days | validity | Expiry_date |
# | ---------- | ------------ | -------------- | -------- | ----------- |
# | R201623    | 20200511     | 1              | online   | 2020-05-12  |
# | R201873    | 20200119     | 110            | online   | 2020-05-08  |
# | R201999    | 20200105     | 35             | online   | 2020-02-09  |
# | R201951    | 20191105     | 215            | online   | 2020-06-07  |


data = [
    ("R201623", 20200511, 1, "online"),
    ("R201873", 20200119, 110, "online"),
    ("R201999", 20200105, 35, "online"),
    ("R201951", 20191105, 215, "online")
]

columns = ["RechargeId", "Rechargedate", "Remaining_days", "validity"]

df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------


