# Databricks notebook source
# Merge Two Files with Different Schemas into One DataFrame
# Coding Question (Q2)
# You are given two files with different schemas:
# File 1: Name | Age
# File 2: Name | Age | Gender
# Write PySpark code to merge both files into a single DataFrame with schema:
# Name, Age, Gender
# For records coming from File 1, Gender should be NULL.

# Input File1
# | Name              | Age |
# | ----------------- | --- |
# | Azarudeen, Shahul | 25  |
# | Michel, Clarke    | 26  |
# | Virat, Kohli      | 28  |
# | Andrew, Simond    | 37  |

# Input File2
# | Name             | Age | Gender |
# | ---------------- | --- | ------ |
# | Rabindra, Tagore | 32  | Male   |
# | Madona, Laure    | 59  | Female |
# | Flintoff, David  | 12  | Male   |
# | Ammie, James     | 20  | Female |



