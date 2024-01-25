#!/bin/bash

# Run Apache Hadoop and Apache Spark services.
docker compose up -d

# Setup python environment for raw data download.
python -m venv env
source env/bin/activate
pip install -r requirements.txt

# Setup folders for keeping the raw data.
SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
mkdir -p $SCRIPTPATH/data/stocks

# Download raw data to the set-up folders.
time python download_data.py

# Save raw data in Apache Hadoop file system.
docker cp $SCRIPTPATH/data namenode:/home
docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave"
docker exec -it namenode bash -c "hdfs dfs -mkdir /data"
docker exec -it namenode bash -c "hdfs dfs -mkdir /data/stocks"
docker exec -it namenode bash -c "hdfs dfs -mkdir /data/curated"
docker exec -it namenode bash -c "hdfs dfs -put -t 12 /home/data/stocks/* /data/stocks"

# Submit periodic query.
docker cp $SCRIPTPATH/query.py spark-master:/home
docker exec -it spark-master bash -c "/spark/bin/spark-submit /home/query.py"

# Submit queries.
