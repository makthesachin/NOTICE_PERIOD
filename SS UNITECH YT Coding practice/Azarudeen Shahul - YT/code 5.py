# Databricks notebook source
# Q5 – Problem Statement

# Given a CSV file, compare the output of map vs flatMap and find the result of count().

# Input
# 101,Azar,finance
# 102,Mani,HR
# 103,Raj,IT

# Question 1:
# in_rdd = sc.textFile("input.csv")
# map_rdd = in_rdd.map(lambda x: x.split(','))
# map_rdd.count()

# question 2
# in_rdd = sc.textFile("input.csv")
# map_rdd = in_rdd.flatMap(lambda x: x.split(','))
# map_rdd.count()


data = [
    ("101,Azar,finance",),
    ("102,Mani,HR",),
    ("103,Raj,IT",)
]

df = spark.createDataFrame(data, ["value"])
df.show(truncate=False)
