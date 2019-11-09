import boto3
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def update_to_dynamo(tweet_id, sentiment):
    response = table.update_item(
        Key={
            'id': tweet_id
        },
        UpdateExpression='set sentiment = :vall',
        ExpressionAttributeValue={
            ':vall': sentiment,
        }
    )
    return response, tweet_id

def lambda_handler(event, context):
    comprehend = boto3.client('comprehend')
    client = boto3.resource('dynamodb')
    table = client.Table('stream_tweets')

    logger.info(f"event {event}")
    event_json = json.dumps(event)
    logger.info(f"event str {event_json}")
    tweet_id = event['Records'][0]['dynamodb']['NewImage']['id']['S']
    tweet_text = event['Records'][0]['dynamodb']['NewImage']['text']['S']

    sentiment = comprehend.detect_sentiment(Text=tweet_text,
                                                 LanguageCode='en')
    logger.info(f"sentiment {sentiment}")
    pos_sentiment = sentiment['SentimentScore']['Positive']

    response = update_to_dyunamo(tweet_id, pos_sentiment)

    return True


