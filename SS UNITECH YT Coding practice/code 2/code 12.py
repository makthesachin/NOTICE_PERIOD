# Databricks notebook source
# | id |
# | -- |
# | 1  |
# | 2  |
# | 3  |
# | 4  |
# | 5  |

# | id |
# | -- |
# | 4  |
# | 5  |
# | 6  |
# | 7  |
# | 8  |
# | 9  |
# | 10 |

# | id |
# | -- |
# | 1  |
# | 2  |
# | 3  |
# | 4  |
# | 5  |
# | 6  |
# | 7  |
# | 8  |
# | 9  |
# | 10 |


data1 = [(1,), (2,), (3,), (4,), (5,)]
data2 = [(4,), (5,), (6,), (7,), (8,), (9,), (10,)]

df1 = spark.createDataFrame(data1, ["id"])
df1.show()
df2 = spark.createDataFrame(data2, ["id"])
df2.show()
