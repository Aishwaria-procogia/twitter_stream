import pandas as pd
import json
import os
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

        #Write .json file for all data fields
        with open('tweets.json', 'a') as f:
            f.write(json.dumps(all_data))
            f.write('\n')

    def on_error(self, status):
        print (status)

if __name__=='__main__':
    runtime = 20
    with open('tweets.json', 'w') as f:
        f = {}

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    stream = Stream(auth, l)
    stream.filter(track=['aws', '#aws', 'datascience', '#datascience', '#python', 'python'], async = True)
    time.sleep(runtime) #halts the control for runtime seconds

    stream.disconnect()

    # Create an S3 client
    s3 = boto3.client('s3')

    filename = 'tweets.json'
    bucket_name = 'tweet-test-321'

    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    s3.upload_file(filename, bucket_name, filename)

