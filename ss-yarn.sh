#!/usr/bin/env bash

sudo -u hdfs hdfs dfs -rm -r -skipTrash /trips/*
sudo -u hdfs hdfs dfs -mkdir /trips
sudo -u hdfs hdfs dfs -chmod a+rw /trips

spark-submit\
 --conf spark.app.id=MultipleOutput\
 --name MultipleOutput\
 --master yarn-client\
 --executor-cores 2\
 --num-executors 1\
 --executor-memory 2G\
 target/spark-multi-output-0.1.jar\
 trips hdfs:///trips /tmp/alter-table.hql
