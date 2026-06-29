#!/bin/bash

# Train both anomaly detection models:
# 1. KMeans
# 2. Bisecting KMeans

source ./env.sh

JAR="target/scala-2.11/anomalyDetection-assembly-1.0.jar"
DATA="resources/training.csv"

echo "Training KMeans and Bisecting KMeans models..."

if [ ! -f "$JAR" ]; then
  echo "ERROR: JAR not found: $JAR"
  echo "Run: source ./env.sh && sbt assembly"
  exit 1
fi

if [ ! -f "$DATA" ]; then
  echo "ERROR: training data not found: $DATA"
  exit 1
fi

rm -rf clustering threshold clustering_bisect threshold_bisect

echo "Training KMeans..."
spark-submit \
  --class es.dmr.uimp.clustering.KMeansClusterInvoices \
  --master local[*] \
  "$JAR" \
  "$DATA" \
  clustering \
  threshold

echo "Training Bisecting KMeans..."
spark-submit \
  --class es.dmr.uimp.clustering.BisectingKMeansClusterInvoices \
  --master local[*] \
  "$JAR" \
  "$DATA" \
  clustering_bisect \
  threshold_bisect

echo "Training finished."
