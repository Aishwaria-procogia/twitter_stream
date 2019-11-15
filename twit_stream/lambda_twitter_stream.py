import json
import time
import boto3
import base64
import os
from botocore.exceptions import ClientError

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_key =  os.environ['ACCESS_TOKEN']
access_secret = os.environ['ACCESS_SECRET']
#hashtags = os.environ['HASHTAGS']
team1_hashtags = os.environ['TEAM1_HASHTAGS']
team2_hashtags = os.environ['TEAM2_HASHTAGS']

hashtags = list(set(team1_hashtags.split(',') + team2_hashtags.split(',')))


class StdOutListener(StreamListener):

    def on_status(self, status):
        logger.info(f"status {status.text}")

    def on_data(self, data):
        all_data = json.loads(data)
        logger.info(f"all_data {all_data}")
        client = boto3.resource('dynamodb')
        table = client.Table('stream_tweets')
        response = table.put_item(Item={'id':str(all_data['id']) if all_data['id'] else None,
                                        'user':all_data['user']['screen_name']  if
                                        all_data['user']['screen_name'] else None,
                                        'created_at':all_data['created_at']  if
                                        all_data['created_at'] else None,
                                        'text':all_data['text']  if
                                        all_data['text'] else None,
                                        'hashtags':set(hashtags)
                                       })

    def on_error(self, status):
        print (status)

def lambda_handler(event, context):
    runtime = 110
    logger.info(f"lambda connected...")
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    stream = Stream(auth, l)
    stream.filter(track=hashtags, async = True)
    time.sleep(runtime) #halts the control for runtime seconds

    stream.disconnect()

