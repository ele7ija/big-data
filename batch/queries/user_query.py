#!/usr/bin/python

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, Row, Column
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, FloatType
from pyspark.sql.window import Window

if len(sys.argv) != 4:
    print('Usage: python user_query.py STOCK[,STOCK] START_DATE END_DATE')
    sys.exit(1)
TICKERS = sys.argv[1].split(',')
START_DATE = sys.argv[2]
END_DATE = sys.argv[3]

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
    .appName("Stocks statistics: Subset of cumulative stock return") \
    .getOrCreate()
quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

fullpath = "/curated/cumulative_total_stock_return_pivot.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(HDFS_NAMENODE + fullpath)

cols = ['Date']
cols.extend(TICKERS)
df = df.select(cols)

df = df.filter(F.col('Date').between(datetime.strptime(START_DATE, "%Y-%m-%d"), datetime.strptime(END_DATE, "%Y-%m-%d")))
df = df.orderBy('Date')

for t in TICKERS:
    first = df.first()[t]
    df = df.withColumn(t, F.col(t) / F.lit(first))

df.coalesce(1).write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/query/query.csv")

df.show(1)