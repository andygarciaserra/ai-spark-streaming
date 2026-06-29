#!/bin/bash

source ./env.sh

FILE="../resources/production.csv"
JAR="target/scala-2.11/anomalyDetection-assembly-1.0.jar"

echo "Sending production data to Kafka..."

if [ ! -f "$JAR" ]; then
  echo "ERROR: JAR not found: $JAR"
  echo "Run: source ./env.sh && sbt assembly"
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "ERROR: production data not found: $FILE"
  exit 1
fi

java \
  -classpath "$JAR" \
  es.dmr.uimp.simulation.InvoiceDataProducer \
  "$FILE" \
  purchases \
  localhost:9092
