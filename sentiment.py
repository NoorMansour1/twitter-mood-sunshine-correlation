from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from pattern.nl import polarity
from datetime import datetime
import json

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')

tweet_schema_old = spark.read.json(f'/data/twitterNL/201101/20110101-01.out.gz').schema
tweet_schema_new = spark.read.json(f'/data/twitterNL/201609/20160930-13.out.gz').schema # Take only the first hour to get the schema, might speed up processing, schema seems to have changed in september 2014

# Load the data with two different schemas and merge them:
df_old = spark.read.schema(tweet_schema_old).json(f'/data/twitterNL/201[0-4]*/*.out.gz').sample(0.05)\
         .select(to_timestamp(col('created_at'), format='EEE MMM dd HH:mm:ss Z yyyy').alias('time'), 'text')
df_new = spark.read.schema(tweet_schema_new).json(f'/data/twitterNL/201[4-6]*/*.out.gz').sample(0.05)\
         .select(to_timestamp(from_unixtime(col('timestamp_ms')/1000)).alias('time'), 'text')
df = df_new.union(df_old)


def sentiment_tweet(tweet_text):
    if tweet_text is not None and len(tweet_text) > 1:
        tweet_sentiment = polarity(str(tweet_text))
    else:
        tweet_sentiment = 0.0
    return tweet_sentiment

sent_udf = udf(sentiment_tweet, FloatType())
sent_df = df.withColumn('sentiment', sent_udf(col('text'))) # Apply sentiment analysis and convert epoch time to datetime
sent_df = sent_df.groupBy(window('time', '1 hour').alias('window')).agg(mean('sentiment').alias('sentiment'), count('text').alias('num_tweets')) # Group in one hour bins
sent_df = sent_df.select(col('window').end.alias('time'), col('sentiment'), col('num_tweets')) # Use the ending time to define the bin to match the sunshine data
# sent_df = sent_df.withColumn('year', year(col('time'))) \
#                  .withColumn('month', month(col('time')))
sent_df = sent_df.orderBy(col('time').asc()) # Sort by time before writing it
sent_df.coalesce(1).write.mode('overwrite').csv('/user/s2154854/twitterNLcsv', compression='gzip') # Probably small enough to store in one file
