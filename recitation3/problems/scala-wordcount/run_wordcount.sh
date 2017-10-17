#!/usr/bin/env bash
USER=morpheus
EXAMPLE_DIR=/user/$USER/scala-wordcount
hdfs dfs -rm -r $EXAMPLE_DIR/input
hdfs dfs -rm -r $EXAMPLE_DIR/output
hdfs dfs -test -e $EXAMPLE_DIR/input || hdfs dfs -mkdir -p $EXAMPLE_DIR/input
hdfs dfs -copyFromLocal ./input/input.txt $EXAMPLE_DIR/input
spark-submit --master local[2] \
    target/scala-2.11/wordcount_2.11-1.0.2.jar
hdfs dfs -cat $EXAMPLE_DIR/output/part-00000
hdfs dfs -copyToLocal $EXAMPLE_DIR/output/part-00000 ./output

