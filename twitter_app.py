"""
    This script connects to Twitter Streaming API, gets tweets with '#' and
    forwards them through a local connection in port 9009. That stream is
    meant to be read by a spark app for processing. Both apps are designed
    to be run in Docker containers.

    To execute this in a Docker container, do:

        docker run -it -v $PWD:/app --name twitter -p 9009:9009 python bash

    and inside the docker:

        pip install tweepy
        python twitter_app.py

    For more instructions on how to run, refer to final slides in tutorial 8

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Author: Tilemachos Pechlivanoglou

"""

# from __future__ import absolute_import, print_function

import socket
import sys
import json

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream

# Replace the values below with yours
consumer_key = "bR3QWEC1D8sLaHCcBjCk7k9qR"
consumer_secret = "F2MuqdHUJzM9MNxLl5v4cF5QHlz4kPYrGHFEiPnIZ74p1UYSrY"
access_token = "1062904234092388357-U8JomtQmb8Bu8CPm99Gx1IAcLrpTLH"
access_token_secret = "OWKedreNa8kvZGqnVTn0he3vgS7shXJxNIA8aK5FRaFOP"


class TweetListener(StreamListener):
    def on_data(self, data):
        try:
            global conn
            full_tweet = json.loads(data)
            tweet_text = full_tweet['text']
            print("------------------------------------------")
            print(tweet_text + '\n')
            conn.send(str.encode(tweet_text + '\n'))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
        return True

    def on_error(self, status):
        print(status)


# ==== setup local connection ====

# IP and port of local machine or Docker
TCP_IP = socket.gethostbyname(socket.gethostname())  # returns local IP
TCP_PORT = 9009

# setup local connection, expose socket, listen for spark app
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")

# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting getting tweets.")

# ==== setup twitter connection ====
listener = TweetListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, listener)

# setup search terms

track_tags = ['#climatechange', '#climate', '#climateaction', '#change', '#globalwarming', '#environment','#carbon', '#today' , '#action', '#nature',
              '#exam', '#study', '#school', '#university', '#student', '#motivation', '#follow', '#coffee', '#love','#studygram',
              '#marketing', '#marketingtips', '#strategy', '#promotion', '#mobilemarketing', '#mktg', '#b2bmarketing', '#digitalmarketing', '#competition', '#socialmedia',
              '#cryptocurrency', '#bitcoin', '#blockchain', '#crypto', '#trading', '#price', '#ethereum','#coin', '#litecoin', '#airdrop',
              '#cannabis', '#marijuana', '#weed', '#thc', '#ganja', '#weedporn', '#maryjane', '#stoner','#cannabiscommunity', '#weedstagram'
              ]


track = track_tags

language = ['en']

locations = [-130, -20, 100, 50]

# get filtered tweets, forward them to spark until interrupted
try:
    stream.filter(track=track, languages=language, locations=locations)
except KeyboardInterrupt:
    s.shutdown(socket.SHUT_RD)
