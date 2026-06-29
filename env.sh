#!/bin/bash

export JAVA_HOME=$HOME/opt/java/jdk8u492-b09
export SPARK_HOME=$HOME/MasterIA/BigData/SparkStreaming/spark-2.3.1-bin-hadoop2.7
export KAFKA_HOME=$HOME/MasterIA/BigData/SparkStreaming/kafka_2.11-0.8.2.1

export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$KAFKA_HOME/bin:$PATH
