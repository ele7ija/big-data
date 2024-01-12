#!/bin/bash

docker compose up -d

python -m venv env
source env/bin/activate
pip install -r requirements.txt

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
mkdir -p $SCRIPTPATH/data/stocks

time python download_data.py

docker cp $SCRIPTPATH/data namenode:/home
docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave"
docker exec -it namenode bash -c "hdfs dfs -mkdir /data"
docker exec -it namenode bash -c "hdfs dfs -put -t 12 /home/data/* /data"

docker cp $SCRIPTPATH/query.py spark-master:/home
docker exec -it spark-master bash -c "/spark/bin/spark-submit /home/query.py"
