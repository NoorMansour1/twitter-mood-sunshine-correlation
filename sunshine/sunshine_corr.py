rom pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pattern.nl import polarity
from datetime import datetime
import json

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')

schema = StructType() \
        .add('time', TimestampType(), True) \
        .add('sentiment', DoubleType(), True) \
        .add('num_tweets', IntegerType(), True) \
        .add('year', IntegerType(), True) \
        .add('month', IntegerType(), True)

sent_df = spark.read.format('csv').schema(schema).load('/user/s2065630/twitterNLcsv/')
sent_df.show()


sun_df = spark.read.csv('file:///home/s2065630/project/MBD-twitter-project/sunshine/sunshine2016.csv', header=True, inferSchema=True, timestampFormat='yyyy-MM-dd HH:mm:ss')
#sun_df = sun_df.filter(sun_df['Time'] < datetime(2016, 1, day+1))

final_df = sent_df.join(sun_df, sent_df.time == sun_df.time).drop(sun_df.time).drop('month')
# final_df.show(25)
print('Corr over raw data:', final_df.corr('sentiment', 'radiation'))
print('Corr over 7d moving average sunshine:', final_df.corr('sentiment', '7d_ma'))
print('Corr over 30d moving average sunshine:', final_df.corr('sentiment', '30d_ma'))
final_df.orderBy(col('num_tweets').asc()).show(25) # Show the lowest counts per hour so we have an idea how much to sample
