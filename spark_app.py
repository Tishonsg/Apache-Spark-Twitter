
import re
import time
import json
from xmlrpc.client import Boolean

from textblob import TextBlob
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests

# create spark configuration
 conf = SparkConf()
 conf.setAppName("GunViolence")
# create spark context with the above configuration
 sc = SparkContext(conf=conf)
 sc.setLogLevel("ERROR")

text_file = sc.textFile("")
counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://...")





