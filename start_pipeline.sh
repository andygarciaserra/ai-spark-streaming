#!/bin/bash

source ./env.sh

JAR="target/scala-2.11/anomalyDetection-assembly-1.0.jar"

echo "Starting Spark Streaming pipeline..."

if [ ! -f "$JAR" ]; then
  echo "ERROR: JAR not found: $JAR"
  echo "Run: source ./env.sh && sbt assembly"
  exit 1
fi

if [ ! -d "clustering" ] || [ ! -f "threshold" ]; then
  echo "ERROR: KMeans model or threshold not found."
  echo "Run: ./start_training.sh"
  exit 1
fi

if [ ! -d "clustering_bisect" ] || [ ! -f "threshold_bisect" ]; then
  echo "ERROR: Bisecting KMeans model or threshold not found."
  echo "Run: ./start_training.sh"
  exit 1
fi

spark-submit \
  --class es.dmr.uimp.realtime.InvoicePipeline \
  --master local[4] \
  "$JAR" \
  ./clustering \
  ./threshold \
  ./clustering_bisect \
  ./threshold_bisect \
  localhost:2181 \
  pipeline \
  purchases \
  2 \
  localhost:9092
