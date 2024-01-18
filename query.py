#!/usr/bin/python

import os

from pyspark.sql import SparkSession, Row, Column
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, FloatType
from pyspark.sql.window import Window

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .master('spark://spark-master:7077') \
    .appName("Stocks statistics") \
    .getOrCreate()
quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# fullpath = "/data/stocks/*.csv"
# all_rows = spark.read.format("csv") \
#     .option("header", "true") \
#     .load(HDFS_NAMENODE + fullpath) \
#     .withColumn("filename", F.input_file_name())
# print("There are total of %d rows." % all_rows.count())
# print("First 5 rows: %s." % all_rows.head(5))

ntnx = spark.read.format("csv") \
    .option("header", "true") \
    .load(HDFS_NAMENODE + "/data/stocks/NTNX.csv")

w1 = Window.orderBy(F.col('Date').cast(TimestampType()))
w2 = Window.orderBy(F.col('Date').cast(TimestampType()))
w3 = Window.orderBy(F.col('Date').cast(TimestampType()))
df = ntnx.select('Date', 'Close')
first = df.first().Close
df = df.withColumn('gain', F.col('Close') / F.lag('Close', 1, first).over(w1))
# df = df.withColumn('cumprod', F.lit(1))
# df.show(10)
# df = df.withColumn('cumprod', F.lag('cumprod').over(w2) * (F.lag('gain').over(w3) + 1))
# # df = df.withColumn('cumprod', F.aggregate('gain', initialValue, merge))

#define window for calculating cumulative sum
my_window = (Window.orderBy(F.col('Date').cast(TimestampType())))

#create new DataFrame that contains cumulative sales column
wind = Window.rangeBetween(Window.unboundedPreceding, Window.currentRow).orderBy("Date")
# df = df.withColumn('cum', F.collect_list("gain").over(wind))
# df = df.withColumn('cumprod', F.aggregate('cum', F.lit(1.0), lambda acc, x: acc * x))

df = df.withColumn('cum', F.product('gain').over(wind))

fullpath = "/data/stocks/*.csv"
all_rows = spark.read.format("csv") \
    .option("header", "true") \
    .load(HDFS_NAMENODE + fullpath) \
    .withColumn("Ticker", F.regexp_extract(F.input_file_name(), "\/(\w+)\.csv", 1))

df = all_rows.select('Date', 'Close', 'Ticker')
w1 = Window.partitionBy('Ticker').orderBy(F.col('Date').cast(TimestampType()))
w2 = Window.partitionBy('Ticker').orderBy(F.col('Date').cast(TimestampType()))
w3 = Window.partitionBy('Ticker').orderBy(F.col('Date').cast(TimestampType()))
first = df.first().Close
df = df.withColumn('gain', F.when( F.row_number().over(w1) != 1, F.col('Close') / F.lag('Close', 1, first).over(w1)).otherwise(F.lit(1.0)))

wind = Window.partitionBy('Ticker').rangeBetween(Window.unboundedPreceding, Window.currentRow).orderBy("Date")
df = df.withColumn('cum', F.product('gain').over(wind))

# Pivot by same values of date and keep like that.
# df.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/cumulative_total_stock_return.csv")

# TODO transform - throw out ETF in name
# TODO programmatically choose N stocks and generate plot.
# df = df.filter(F.col('Ticker').isin('NTNX', 'MSFT', 'AAPL')).filter(F.col('Date').between('2018-01-01', '2018-02-28'))


df.show(100)