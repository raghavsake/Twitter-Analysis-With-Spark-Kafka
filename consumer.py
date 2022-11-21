from __future__ import print_function
from pyspark.sql import functions as f
import argparse
import json
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.functions import *
import logging
import time


import os

os.environ['PYSPARK_SUBMIT_ARGS'] = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0'

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = re.sub(r'\n+', ' ', str(tweet))
    tweet = tweet.strip('[link]')

    return tweet

def main():

    broker = "localhost:9092"
    topics = "dbt_project"
    batch_duration = 2

    spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .master("local[*]") \
    .config('spark.port.maxRetries', 100) \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark \
        .readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dbt_project") \
        .option("startingOffsets", "earliest") \
        .load()

    
    logging.info("started to listen to the host..")
    mySchema = StructType([StructField("text", StringType(), True)])
    values = df.select(from_json(df.value.cast("string"), mySchema).alias("tweet"))
    df1 = values.select("tweet.*")
    clean_tweets = f.udf(cleanTweet, StringType())
    df1.withColumn('tweets', clean_tweets(col("text")))

    query1 = df1.writeStream.queryName("counting").format("memory").outputMode("append").start()
    for x in range(2):
        spark.sql("select count(*) from counting").show()
        spark.sql("select * from counting").show()
        time.sleep(batch_duration)

    df1= df1.select(f.split("text", " ").alias("text"),f.posexplode(f.split("text", " ")).alias("pos", "val"))
    df1=df1.select(f.expr("text[pos]").alias("words"))


    query1 = df1.writeStream.queryName("grp").format("memory").outputMode("append").start()
    for x in range(2):
        spark.sql("select count(*) from grp").show()
        spark.sql("select * from grp").show()
        time.sleep(batch_duration)

    query1 = df1.writeStream.queryName("countingWords").format("memory").outputMode("append").start()
    for x in range(2):
        spark.sql("select count(*) from countingWords where words = 'RT'").show()
        time.sleep(batch_duration)

    query1 = df1.writeStream.queryName("countingRCB").format("memory").outputMode("append").start()
    for x in range(2):
        spark.sql("select count(*) from countingRCB where words = 'AB'").show()
        time.sleep(batch_duration)


    print(broker, topics, type(batch_duration))

if __name__ == "__main__":
    main()
