#!/usr/bin/python

import os

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
all_rows = spark.read.format("csv") \
    .option("header", "true") \
    .load(HDFS_NAMENODE + fullpath) \
    .withColumn("filename", input_file_name())
print("There are total of %d rows." % all_rows.count())
print("First 5 rows: %s." % all_rows.head(5))