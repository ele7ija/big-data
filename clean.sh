#!/bin/bash

docker exec -it spark-master bash -c "rm -rf /home/query.py"

docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data*"
docker exec -it namenode bash -c "rm -rf /home/data"

SCRIPT=$(realpath "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
rm -rf $SCRIPTPATH/data

rm -rf env

docker compose down