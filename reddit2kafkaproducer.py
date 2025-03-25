import pandas as pd
import praw
import re
from nltk.sentiment import SentimentIntensityAnalyzer
import requests
from kafka import KafkaProducer
import json

client_id='1zdxUrNbpnIjYUCiNU7RqA'
client_secret='Zzzg_6jdIrJJSSVv0Tv-ZScKXpwa0g'

auth = requests.auth.HTTPBasicAuth(client_id,client_secret)

with open('C:/Users/bhati/PycharmProjects/BigDataHW/pw.txt', 'r') as f:
    pw=f.read()

data_red = {"grant_type": "password", "username": "Vidushi4", "password": pw }

headers = {'User-Agent': 'learningkafka/0.0.1'}

res = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data_red,headers=headers)

TOKEN = res.json()['access_token']
print(TOKEN)
headers['Authorization'] = f'bearer {TOKEN}'
print(headers)

res = requests.get('https://oauth.reddit.com/r/kafka/new', headers=headers)
topic_name = 'reddit_posts_sentiments'

# Initialize Kafka producer (assuming Kafka and Zookeeper are running on localhost:9092)
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

#def clean_text(text):
#    """Remove URLs from a given text string."""
#    return re.sub(r'http\S+', '', text)

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

# def analyze_sentiment_with_vader(text):
#     """Perform sentiment analysis on the given text using VADER."""
#     sia = SentimentIntensityAnalyzer()
#     sentiment = sia.polarity_scores(text)
#     return sentiment

for post in res.json()['data']['children']:
    if post['data']['selftext'] != '':
        print("Title :", post['data']['title'])
        id = post['kind'] + '_' + post['data']['id']
        subredditvar = post['data']['subreddit']
        title = post['data']['title'],
        cleaned_selftext = clean_text(post['data']['selftext'])
        #sentiment = analyze_sentiment_with_vader(cleaned_selftext)
        print(f"Processed Text: {cleaned_selftext}")
        #print(f"Sentiment: {sentiment}")

        post_data = {
            'id' : id,
            'subredditvar': subredditvar,
            'title': title,
            'selftext': cleaned_selftext,
            #'sentiment': sentiment
                    }

        # Send data to Kafka topic
        producer.send(topic_name, value=post_data)
        print(f"Sent post {post_data} to Kafka topic {topic_name}.")
