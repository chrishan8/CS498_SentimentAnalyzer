#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  4 18:04:09 2018

@author: no-face
"""

import io.archivesunleashed.spark.matchbox._
import io.archivesunleashed.spark.matchbox.TweetUtils._
import io.archivesunleashed.spark.rdd.RecordRDD._

## Load tweets from HDFS
val tweets = RecordLoader.loadTweets("path to tweets", sc)

## Count them
tweets.count()

## Extract some fields
val r = tweets.map(tweet => (tweet.id, tweet.createdAt, tweet.username, tweet.text, tweet.lang,
                             tweet.isVerifiedUser, tweet.followerCount, tweet.friendCount))

##  Take a sample of 10 on console
r.take(10)

## Count the number of hashtags

val hashtags = tweets.map(tweet => tweet.text)
                     .filter(text => text != null)
                     .flatMap(text => {"""#[^ ]+""".r.findAllIn(text).toList})
                     .countItems()

hashtags.take(10)

df = (
    input_df
    .withColumn("tokens", tokenize_udf(input_df['body']))
    .withColumn("state", extract_state(input_df['gnip.profileLocations']))
    .filter('state IS NOT NULL')
    .filter(is_from_official_client(input_df['generator.displayName']))
    .withColumn("user_id", remove_user_id_prefix(input_df['actor.id']))
)
df_tokens = df.select(df.id, df.user_id, F.explode(df.tokens).alias('word'))
df_tokens.cache()

df_topics_by_state = (
    df_tokens
    .join(df_topic_words, on='word')
    .join(df_user_states, on='user_id')
    .select('user_id', 'topic', 'state')
    .groupby(['state', 'topic'])
    .agg(F.countDistinct('user_id').alias('n_users'))
)

df_topics_by_state.cache()