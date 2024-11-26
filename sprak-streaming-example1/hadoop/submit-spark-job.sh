#!/bin/bash

docker exec spark-master spark-submit \
  --class ma.enset.IncidentsAnalysis \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  /opt/bitnami/spark/apps/sprak-streaming-example1-1.0-SNAPSHOT.jar
