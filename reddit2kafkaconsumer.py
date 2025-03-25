from kafka import KafkaConsumer
import json

# Initialize a Kafka Consumer
consumer = KafkaConsumer(
    'reddit_posts_sentiments', # replace with your topic name
    bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
    auto_offset_reset='earliest', # Start reading at the earliest message
    enable_auto_commit=True,
    group_id='your_consumer_group', # replace with your consumer group
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Listen for messages on the topic
for message in consumer:
    message = message.value
    print("Received message: ", message)
    # Here you can process the message or perform further analysis
