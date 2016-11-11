#!/bin/bash
spark-shell --class com.ucd.spark.recommender.RecommenderApp --jars $(echo /Users/paulkelly/code/spark-streaming/target/pack/lib/*.jar | tr ' ' ',')

