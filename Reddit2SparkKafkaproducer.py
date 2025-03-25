import nltk
nltk.download('vader_lexicon')
import pandas as pd
import praw
import re
from nltk.sentiment import SentimentIntensityAnalyzer
import requests
from kafka import KafkaProducer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
# from kafka import KafkaProducer
# import json
from pyspark.sql import Row


client_id='1zdxUrNbpnIjYUCiNU7RqA'
client_secret='Zzzg_6jdIrJJSSVv0Tv-ZScKXpwa0g'

auth = requests.auth.HTTPBasicAuth(client_id,client_secret)

with open('pw.txt', 'r') as f:
    pw=f.read()

data_red = {"grant_type": "password", "username": "Vidushi4", "password": pw }

headers = {'User-Agent': 'learningkafka/0.0.1'}

res = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data_red,headers=headers)

TOKEN = res.json()['access_token']
print(TOKEN)
headers['Authorization'] = f'bearer {TOKEN}'
print(headers)

res = requests.get('https://oauth.reddit.com/r/python/new', headers=headers)
topic_name = 'reddit_posts_sentiments'

TAG_RE = re.compile(r'<[^>]+>')

def remove_tags(text):
    #print(text)
    return TAG_RE.sub('', text)

def clean_text(sen):
    #print(sen)
    # Removing html tags
    sentence = remove_tags(sen)
    sentence = re.sub(r'http\S+', '', sentence)  # Remove URLs
    # Remove punctuations and numbers
    sentence = re.sub('[^a-zA-Z]', ' ', sentence)

    # Single character removal
    sentence = re.sub(r"\s+[a-zA-Z]\s+", ' ', sentence)

    # Removing multiple spaces
    sentence = re.sub(r'\s+', ' ', sentence)

    return sentence

# Define a function to classify sentiment based on VADER's compound score
def classify_sentiment(compound_score):
    if compound_score < 0.4:
        return 'negative'
    elif compound_score < 0.7:
        return 'neutral'
    else:
        return 'positive'

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Reddit Sentiment Analysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
    .getOrCreate()


def fetch_reddit_data():
    list_of_posts =[]

    for post in res.json()['data']['children']:
        if post['data']['selftext'] != '':
            print("Title :", post['data']['title'])
            id = post['kind'] + '_' + post['data']['id']
            subredditvar = post['data']['subreddit']
            cleaned_title = clean_text(post['data']['title'])
            cleaned_selftext = clean_text(post['data']['selftext'])

            post_data = {
                'id' : id,
                'subredditvar': subredditvar,
                'title': cleaned_title,
                'selftext': cleaned_selftext,

                        }

            row_data = Row(**post_data)
            list_of_posts.append(row_data)

    return list_of_posts

# Fetch your data (a list of dictionaries)
reddit_data = fetch_reddit_data()

# Parallelize the collection and create a DataFrame
df = spark.createDataFrame(reddit_data)
df.show(n=10)


# Register the function as a UDF
classify_sentiment_udf = udf(classify_sentiment, StringType())

# Define a UDF to compute sentiment using VADER
def compute_sentiment(text):
    sia = SentimentIntensityAnalyzer()
    score = sia.polarity_scores(text)['compound']
    return score

compute_sentiment_udf = udf(compute_sentiment, StringType())
print("Score : ",compute_sentiment_udf )

# Add a new column with the sentiment score
df = df.withColumn("compound_score", compute_sentiment_udf(df['selftext']))

# Classify sentiment based on the compound score
df = df.withColumn("sentiment", classify_sentiment_udf(df['compound_score']))

print(df)

def send_partition_to_kafka(partition):
    from kafka import KafkaProducer
    import json

    # Initialize Kafka Producer once per partition
    producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    for row in partition:
        post_data = row.asDict()
        producer.send(topic_name, value=post_data)
    producer.flush()
    producer.close()

# Apply the function to each partition of the DataFrame
df.rdd.foreachPartition(send_partition_to_kafka)

