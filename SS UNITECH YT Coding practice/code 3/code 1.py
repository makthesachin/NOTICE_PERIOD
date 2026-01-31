# Databricks notebook source
# Load File with Custom Delimiter (~|) into Spark DataFrame
# Coding Question

# Q13.
# You are given a text file where the delimiter is ~|.
# The file also contains:
# A header
# Comma inside the Name field
# Write PySpark code (using SparkSession, not SparkContext) to load this file into a Spark DataFrame with proper columns.

# Input:
# Name~|Age
# Azarudeen, Shahul~|25
# Michel, Clarke~|26
# Virat, Kohli~|28
# Andrew, Simond~|37
# Geogre, Bush~|59
# Flintoff, David~|12
# Adam, James~|20

# output:
# | Name              | Age |
# | ----------------- | --- |
# | Azarudeen, Shahul | 25  |
# | Michel, Clarke    | 26  |
# | Virat, Kohli      | 28  |
# | Andrew, Simond    | 37  |
# | Geogre, Bush      | 59  |
# | Flintoff, David   | 12  |
# | Adam, James       | 20  |


