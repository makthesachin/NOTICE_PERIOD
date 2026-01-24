# Databricks notebook source
# | SOID | SODate     | ItemId | ItemQty | ItemValue |
# | ---: | ---------- | ------ | ------: | --------: |
# |    1 | 01-01-2024 | I1     |      10 |      1000 |
# |    2 | 15-01-2024 | I2     |      20 |      2000 |
# |    3 | 01-02-2024 | I3     |      10 |      1500 |
# |    4 | 15-02-2024 | I4     |      20 |      2500 |
# |    5 | 01-03-2024 | I5     |      30 |      3000 |
# |    6 | 10-03-2024 | I6     |      40 |      3500 |
# |    7 | 20-03-2024 | I7     |      20 |      2500 |
# |    8 | 30-03-2024 | I8     |      10 |      1000 |

# | Year_Month | TotalSale | PercentageDiffPrevMonth |
# | ---------- | --------: | ----------------------: |
# | Jan-24     |      3000 |                    null |
# | Feb-24     |      4000 |                      25 |
# | Mar-24     |     10000 |                      60 |


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sales_data = [
    (1, "2024-01-01", "I1", 10, 1000),
    (2, "2024-01-15", "I2", 20, 2000),
    (3, "2024-02-01", "I3", 10, 1500),
    (4, "2024-02-15", "I4", 20, 2500),
    (5, "2024-03-01", "I5", 30, 3000),
    (6, "2024-03-10", "I6", 40, 3500),
    (7, "2024-03-20", "I7", 20, 2500),
    (8, "2024-03-30", "I8", 10, 1000)
]

df = spark.createDataFrame(
    sales_data,
    ["SOID", "SODate", "ItemId", "ItemQty", "ItemValue"]
)

df.show()
df.createOrReplaceTempView('df')

# COMMAND ----------

# df.withColumn("month_name", date_format(df["date_col"], "MMMM"))

# COMMAND ----------

from pyspark.sql.functions import date_format, year, day, col, to_date, concat_ws

df1 = df.withColumn('month', date_format(to_date(col('SODate')), 'MMM')) \
       .withColumn('year', year(to_date(col('SODate')))) \
       .withColumn('date', day(to_date(col('SODate')))) \
       .withColumn('Year_Month', concat_ws('-', col('month'), col('year'))) \
       .drop('SODate')
display(df1)

# COMMAND ----------

from pyspark.sql.functions import month, to_date, year, col, day, sum as sum_, lag
from pyspark.sql.window import Window

df2 = df1.groupby(col('Year_Month')).agg(sum_(col('ItemValue')).alias('sum_month'))
w = Window.orderBy(col('sum_month'))
df3 = df2.withColumn('prev_sum_month', lag('sum_month').over(w))\
    .withColumn('PercentageDiffPrevMonth', ((col('sum_month') - col('prev_sum_month')) / col('sum_month')) * 100)

display(df3)

# COMMAND ----------

from pyspark.sql.functions import month, to_date, year,col,day,sum as sum_

df2 = df1.groupby(col('Year_Month')).agg(sum_(col('ItemValue')).alias('sum_month'))
w = Window.orderBy(col('sum_month'))
df3 = df2.withColumn('PercentageDiffPrevMonth',lag('sum_month')over(w))

display(df3)

# COMMAND ----------

from pyspark.sql.functions import month, to_date, year, col, day, sum as sum_, lag, round
from pyspark.sql.window import Window

df2 = df1.groupby(col('Year_Month')).agg(sum_(col('ItemValue')).alias('sum_month'))

windowSpec = Window.orderBy('Year_Month')
df3 = df2.withColumn('prev_sum_month', lag('sum_month').over(windowSpec)) \
         .withColumn('PercentageDiffPrevMonth', 
                     round(((col('sum_month') - col('prev_sum_month')) / col('prev_sum_month')) * 100, 0))

display(df3)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import month,to_date,year
df = df.withColumn('month',month(to_date("SODate")).withColumn('year',year(to_date("SODate"))))
df.show()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col,row_number,month,to_date

df = df.withColumn('month',month(to_date(col('SODate'))))
df.show()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col,row_number

windowSpec = (Window.partitionBy(col('SODate')).orderBy(col('ItemQty').desc()))
dfwindow = df.withColumn('ItemValue',row_number().over(windowSpec))
display(dfwindow)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col,row_number

WindowSpec = Window.partitionBy(col('SODate'))
dfwindow = df.withColumn('ItemValue',row_number().over(WindowSpec).orderBy(col('ItemQty').desc()))

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC   month(to_date(SODate)) as month,
# MAGIC   sum(ItemValue) over(
# MAGIC     partition by month(to_date(SODate))
# MAGIC     order by month(to_date(SODate))
# MAGIC   ) as sum_itemvalue,
# MAGIC   (sum(ItemValue) over(
# MAGIC       partition by month(to_date(SODate))
# MAGIC       order by month(to_date(SODate))
# MAGIC     ) - lag(sum(ItemValue) over(
# MAGIC       partition by month(to_date(SODate))
# MAGIC       order by month(to_date(SODate))
# MAGIC     )) over (
# MAGIC       order by month(to_date(SODate))
# MAGIC     )
# MAGIC   ) / lag(sum(ItemValue) over(
# MAGIC       partition by month(to_date(SODate))
# MAGIC       order by month(to_date(SODate))
# MAGIC     )) over (
# MAGIC       order by month(to_date(SODate))
# MAGIC     ) * 100 as pct_diff_sum_itemvalue
# MAGIC from df

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC   month(to_date(SODate)) as month,
# MAGIC   sum(ItemValue) over(
# MAGIC     partition by month(to_date(SODate))
# MAGIC     order by month(to_date(SODate))
# MAGIC   ) as sum_itemvalue
# MAGIC from df

# COMMAND ----------


