import json
import time
import boto3

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_key = os.environ['ACCESS_TOKEN']
access_secret = os.environ['ACCESS_TOKEN_SECRET']


class StdOutListener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)

        table = client.Table('stream_tweets')
        response = table.put_item(Item={'id':all_data['id'] if all_data['id'] else None,
                                        'user':all_data['user']['screen_name']  if
                                        all_data['user']['screen_name'] else None,
                                        'created_at':all_data['created_at']  if
                                        all_data['created_at'] else None,
                                        'text':all_data['text']  if
                                        all_data['text'] else None,
                                        'batch_number':1})

    def on_error(self, status):
        print (status)

def lambda_handler(event, context):
    runtime = 150
    client = boto3.resource('dynamodb')
    table = client.Table('stream_tweets')
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    stream = Stream(auth, l)
    stream.filter(track=['aws', '#aws', 'datascience', '#datascience',
                         '#python', 'python', '#seahawks'], async = True)
    time.sleep(runtime) #halts the control for runtime seconds

    stream.disconnect()

