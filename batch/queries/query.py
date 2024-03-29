#!/usr/bin/python

import os
from datetime import date, timedelta

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
    .config('spark.sql.pivotMaxValues', 15000) \
    .config('spark.driver.memory', '15g') \
    .config('spark.executor.memory', '12g') \
    .appName("Stocks statistics: Curate - Run periodic thread") \
    .getOrCreate()
quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

fullpath = "/transformed/close.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(HDFS_NAMENODE + fullpath)

start = date.today() - timedelta(days=5*365)
df = df.filter(F.col('Date') > start.strftime('%Y-%m-%d'))

w1 = Window.partitionBy('Ticker').orderBy(F.col('Date').cast(TimestampType()))
w2 = Window.partitionBy('Ticker').orderBy(F.col('Date').cast(TimestampType()))
w3 = Window.partitionBy('Ticker').orderBy(F.col('Date').cast(TimestampType()))
first = df.first().Close
df = df.withColumn('gain', F.when( F.row_number().over(w1) != 1, F.col('Close') / F.lag('Close', 1, first).over(w1)).otherwise(F.lit(1.0)))

wind = Window.partitionBy('Ticker').rangeBetween(Window.unboundedPreceding, Window.currentRow).orderBy("Date")
df = df.withColumn('CTSR', F.product('gain').over(wind))

df = df.drop('gain')

# Pivot by same values of date and keep like that
df = df.groupBy('Date').pivot('Ticker').agg(F.first('CTSR')).orderBy('Date')

df.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/cumulative_total_stock_return_pivot.csv")

df.show(1)