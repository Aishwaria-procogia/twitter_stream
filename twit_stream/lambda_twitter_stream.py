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
team1_hashtags = os.environ['TEAM1_HASHTAGS']
team2_hashtags = os.environ['TEAM2_HASHTAGS']

team1_hashtags = team1_hashtags.split(',')
team2_hashtags = team2_hashtags.split(',')

team1_hashtags_to_dynamo = set([t.lower().replace('#','') for t in team1_hashtags])
team2_hashtags_to_dynamo = set([t.lower().replace('#','') for t in team2_hashtags])

                            
hashtags = list(set(team1_hashtags + team2_hashtags))


class StdOutListener(StreamListener):

    def on_status(self, status):
        logger.info(f"status {status.text}")

    def on_data(self, data):
        tweet_hashtags =[]
        all_data = json.loads(data)
        
        try:
            tweet_text = all_data['extended_tweet']['full_text']
        except:
            tweet_text = all_data['text']
            
        
        try:
            hashtags = all_data['entities']['hashtags']
        except:
            pass
        if len(hashtags) == 0:
            try:
                hashtags = all_data['quoted_status']['extended_tweet']['entities']['hashtags']
            except:
                pass
        if len(hashtags) == 0:
            try:
                hashtags = all_data['quoted_status']['entities']['hashtags']
            except:
                pass  
        if len(hashtags) == 0:
            try:
                hashtags = all_data['extended_tweet']['entities']['hashtags']
            except:
                pass
        
        for tag in hashtags:
            tweet_hashtags.append(tag['text'].lower().replace('#',''))
            
        tweet_hashtags = set(tweet_hashtags)
            
        logger.info(f"all_data {all_data}")
        logger.info(f"tweet text {tweet_text}")
        logger.info(f"hash tags {tweet_hashtags}")
        client = boto3.resource('dynamodb')
        table = client.Table('stream_tweets')
        response = table.put_item(Item={'id':str(all_data['id']) if all_data['id'] else None,
                                        'user':all_data['user']['screen_name']  if
                                        all_data['user']['screen_name'] else None,
                                        'created_at':all_data['created_at']  if
                                        all_data['created_at'] else None,
                                        'tweet_text':tweet_text  if
                                        tweet_text else None,
                                        'team1_hashtags': team1_hashtags_to_dynamo,
                                        'team2_hashtags': team2_hashtags_to_dynamo,                                        
                                        'tweet_hashtags': tweet_hashtags if tweet_hashtags else None
                                        
                                       })

    def on_error(self, status):
        if status == 420:
            #returning False in on_data disconnects the stream
            return False
        print (f'error: {status}')

def lambda_handler(event, context):
    runtime = 890
    logger.info(f"lambda connected...")
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    stream = Stream(auth, l)
    stream.filter(track=hashtags, is_async = True)
    time.sleep(runtime) #halts the control for runtime seconds

    stream.disconnect()

