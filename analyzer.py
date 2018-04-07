import tweepy
from tweepy.models import ResultSet
from tweepy import OAuthHandler
from tweepy import Stream
import metapy
import sys
import os
from datetime import datetime, date, time
import requests
from bs4 import BeautifulSoup, SoupStrainer
import numpy as np
import numpy.core.umath as umath
import sqlalchemy as sa
import pandas as pd
import itertools
from itertools import product
import scipy as sp
import numpy as np
import metapy
import re
from sklearn.datasets import fetch_20newsgroups
import nltk.classify.util
from nltk.classify import NaiveBayesClassifier
from nltk.classify import SklearnClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import BernoulliNB
from sklearn.ensemble import RandomForestClassifier

access_token = "961774175537987585-ureLaomA1AV14FFTaddhTUGGDmdIsCY"
access_token_secret = "afsplbGj2VxXyg3tIGHvsBMkK5alGHM2LIEh5kgtJu9Pn"
consumer_key = "9cOGyv8vvyNaF3dKWsUm4tVTq"
consumer_secret = "9wTLVJUlJRTjZxJhW7o2ZMfcsgaslh5ikcUcoThSR6bn3WK0qf"

master_dictionary= pd.read_excel("master_dictionary.xlsx")
auth= tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
tweetCount= 20


##the key word features, include the topics that we've discussed 
word_features= ['Donald Trump', 'immigration', 'gun control', 'DACA']

## get the bag of words of each tweet
def get_wordlist(tweet):
    word_list= []
    doc= metapy.index.Document()
    doc.content(tweet.text)
    tok= metapy.analyzers.ICUTokenizer()
    tok= metapy.analyzers.LowercaseFilter(tok)
    tok = metapy.analyzers.LengthFilter(tok, min=2, max=30)
    tok.set_content(doc.content())
    tokens = [wordlist.append(token.upper()) for token in tok]
    return word_list
## get the most popular features out of a word list
 
def get_word_features(wordlist):
    wordlist= nltk.FreqDist(wordlist)
    word_features = wordlist.keys()
    return word_features
    
## rank the word list as positively, negatively, or neutrally sentimented
def get_rating(word_list):
    matches= master_dictionary.loc[master_dictionary['Word'].isin(word_list)]
    negative_words= matches.loc[matches['Negative']!=0]['Word']
    positive_words= matches.loc[matches['Positive']!=0]['Word']
    negative_matches= np.sum(matches['Negative'] !=0)
    positive_matches= np.sum(matches['Positive'] !=0)
    rating = 'Neutral'
    if (negative_matches <= positive_matches):
        rating= 'Positive'
    else:
        rating= 'Negative'
    return rating

## evaluate the documents based on the selected features
def extract_features(document):
    document_words= set(document)
    features = {}
    for word in word_features:
        features['contains(%s)' % word]= (word in document_words)
    return features

if __name__ == '__main__':
    # pulls twenty NYtimes tweets for sample, have to change later
    tweetCount = 20
    name = "nytimes"
    results = api.user_timeline(id=name, count=tweetCount)
    twitter_text= []
    wordlist=[]
    for tweet in results:
        wordlist = get_wordlist(tweet)
        twitter_text.append((wordlist, get_rating(wordlist)))
    training_set= nltk.classify.apply_features(extract_features, twitter_text)
    classifier= nltk.classify.NaiveBayesClassifier.train(training_set)
    classif = SklearnClassifier(BernoulliNB()).train(training_set)
    classif = SklearnClassifier(SVC(), sparse=False).train(training_set)
    classifier_1 = nltk.classify.DecisionTreeClassifier.train(training_set, entropy_cutoff=0, support_cutoff=0)
  
