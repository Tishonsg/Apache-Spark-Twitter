"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:

        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""
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

# VARIABLE DECLARATIONS

ttl_pos_climate = 0
ttl_neg_climate = 0
ttl_neu_climate = 0

ttl_pos_exams = 0
ttl_neg_exams = 0
ttl_neu_exams = 0

ttl_pos_marketing = 0
ttl_neg_marketing = 0
ttl_neu_marketing = 0

ttl_pos_crypto = 0
ttl_neg_crypto = 0
ttl_neu_crypto = 0

ttl_pos_cannabis = 0
ttl_neg_cannabis = 0
ttl_neu_cannabis = 0

# List of hashtag topics

climateChange = ['#climatechange', '#climate', '#climateaction', '#change', '#globalwarming', '#environment',
                 '#carbon', '#today', '#action', '#nature']
exams = ['#exam', '#study', '#school', '#university', '#student', '#motivation', '#follow', '#coffee', '#love',
         '#studygram']
marketing = ['#marketing', '#marketingtips', '#strategy', '#promotion', '#mobilemarketing', '#mktg',
             '#b2bmarketing', '#digitalmarketing', '#competition', '#socialmedia']
cryptocurrency = ['#cryptocurrency', '#bitcoin', '#blockchain', '#crypto', '#trading', '#price', '#ethereum',
                  '#coin', '#litecoin', '#airdrop']
cannabis = ['#cannabis', '#marijuana', '#weed', '#thc', '#ganja', '#weedporn', '#maryjane', '#stoner',
            '#cannabiscommunity', '#weedstagram']


# Performing cleaning of data
def clean_tweet(tweet):
    # Utility function to clean tweet text by removing links, special characters
    # using simple regex statements.
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w+:\ / \ / \S+)", " ", tweet).split())


# Performing sentiment analysis
def get_tweet_sentiment(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    if analysis.sentiment.polarity > 0:
        return 1
    elif analysis.sentiment.polarity == 0:
        return 0
    else:
        return -1


# Categorizing tweet text by sentiment score and category
def categorizing(tweet_text):
    climateChange = ['#climatechange', '#climate', '#climateaction', '#change', '#globalwarming', '#environment',
                     '#carbon', '#today', '#action', '#nature']
    exams = ['#exam', '#study', '#school', '#university', '#student', '#motivation', '#follow', '#coffee', '#love',
             '#studygram']
    marketing = ['#marketing', '#marketingtips', '#strategy', '#promotion', '#mobilemarketing', '#mktg',
                 '#b2bmarketing', '#digitalmarketing', '#competition', '#socialmedia']
    cryptocurrency = ['#cryptocurrency', '#bitcoin', '#blockchain', '#crypto', '#trading', '#price', '#ethereum',
                      '#coin', '#litecoin', '#airdrop']
    cannabis = ['#cannabis', '#marijuana', '#weed', '#thc', '#ganja', '#weedporn', '#maryjane', '#stoner',
                '#cannabiscommunity', '#weedstagram']

    for word in tweet_text.split(' '):
        if word in climateChange:
            sentiment_score = get_tweet_sentiment(tweet_text)
            category = 'Climate'
            return (category,(sentiment_score,1))

        elif word in exams:
            sentiment_score = get_tweet_sentiment(tweet_text)
            category = 'Exams'
            return (category,(sentiment_score,1))

        elif word in marketing:
            sentiment_score = get_tweet_sentiment(tweet_text)
            category = 'Marketing'
            return (category,(sentiment_score,1))

        elif word in cryptocurrency:
            sentiment_score = get_tweet_sentiment(tweet_text)
            category = 'Cryptocurrency'
            return (category,(sentiment_score,1))

        elif word in cannabis:
            sentiment_score = get_tweet_sentiment(tweet_text)
            category = 'Cannabis'
            return (category,(sentiment_score,1))

    return (' ',(0,0))



# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
# ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
# ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter", 9009)

# Map stream to perform sentiment analysis
# tweet_text = dataStream.flatMap(lambda line: line.split(" "))
# categoryAnalysis = categorizing(tweet_text.collect())


# change from tweet text to (topic,sentiment)
topic_sentiments = dataStream.map(categorizing)
sentiment = topic_sentiments.filter(lambda w: ' ' != w[0])


# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, old_value):
    if old_value is None: old_value = (0, 0)
    polarity = sum(value[0] for value in new_values) + old_value[0]
    count = sum(value[1] for value in new_values) + old_value[1]
    return (polarity, count)


# do the aggregation, note that now this is a sequence of RDDs
sentiment_totals = sentiment.updateStateByKey(aggregate_tags_count)



# process a single time interval
def process_interval(time, rdd):
    f = open("result.txt","a")
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort counts (desc) in this time instance and take top 10
        # sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        top10 = rdd.take(10)

        # print it nicely
        for tag in top10:
            avg = float("{:.2f}".format(tag[1][0]/tag[1][1]))
            f.write(str(tag[0] + "\t" + str(avg)) + '\n')
            print('{:<40} {:.2f}'.format(tag[0], tag[1][0]/tag[1][1]))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


sentiment_totals.foreachRDD(process_interval)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
