import boto3
import json
import logging
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def update_to_dynamo(tweet_id, pos_tweet_sentiment, neg_tweet_seniment):
    client = boto3.resource('dynamodb')
    table = client.Table('stream_tweets')

    response = table.update_item(
        Key={
            'id': tweet_id
        },
        UpdateExpression='set positive_sentiment = :pos_sentiment, negative_sentiment =
        :neg_sentiment',
        ExpressionAttributeValues={
            ':pos_sentiment': pos_tweet_sentiment,
            ':neg_sentiment': neg_tweet_sentiment
        }
    )
    return response, tweet_id

def update_to_decimal(obj):
    return decimal.Decimal(obj)

def lambda_handler(event, context):
    comprehend = boto3.client('comprehend')

    logger.info(f"event {event}")
    tweet_id = event['Records'][0]['dynamodb']['NewImage']['id']['S']
    tweet_text = event['Records'][0]['dynamodb']['NewImage']['text']['S']

    sentiment = comprehend.detect_sentiment(Text=tweet_text,
                                                 LanguageCode='en')
    logger.info(f"sentiment {sentiment}")
    pos_sentiment = sentiment['SentimentScore']['Positive']
    pos_sentiment = update_to_decimal(pos_sentiment)

    neg_sentiment = sentiment['SentimentScore']['Negative']
    neg_sentiment = update_to_decimal(neg_sentiment)

    response = update_to_dynamo(tweet_id, pos_sentiment, neg_sentiment)

    return True


