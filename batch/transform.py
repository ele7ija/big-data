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

fullpath = "/data/stocks/*.csv"
transformed = spark.read.format("csv") \
    .option("header", "true") \
    .load(HDFS_NAMENODE + fullpath) \
    .withColumn("Ticker", F.regexp_extract(F.input_file_name(), "\/(\w+)\.csv", 1)) \
    .select('Date', 'Close', 'Ticker')

transformed.write.format("csv") \
    .mode('overwrite') \
    .option('header','true') \
    .save(HDFS_NAMENODE + "/transformed/close.csv")