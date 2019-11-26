import boto3
import logging
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def update_to_stream_dynamo(tweet_id, pos_tweet_sentiment, neg_tweet_sentiment):
    client = boto3.resource('dynamodb')
    table = client.Table('stream_tweets')

    response = table.update_item(
        Key={
            'id': tweet_id
        },
        UpdateExpression='set positive_sentiment = :pos_sentiment, negative_sentiment = :neg_sentiment',
        ExpressionAttributeValues={
            ':pos_sentiment': pos_tweet_sentiment,
            ':neg_sentiment': neg_tweet_sentiment
        }
    )
    

    return response, tweet_id


def push_team1_metric(pos_value, neg_value):
    cloudwatch = boto3.client('cloudwatch')
    neg_response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName' : 'Negative Sentiment',
                'Dimensions': [
                    {
                        'Name': 'Game_Sentiment',
                        'Value': 'Team_1',
                    },
                ],
                'Unit': 'None',
                'Value': neg_value
            },
        ],
        Namespace = 'Sentiment'
    )

    cloudwatch = boto3.client('cloudwatch')
    pos_response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName' : 'Positive Sentiment',
                'Dimensions': [
                    {
                        'Name': 'Game_Sentiment',
                        'Value': 'Team_1',
                    },
                ],
                'Unit': 'None',
                'Value': pos_value
            },
        ],
        Namespace = 'Sentiment'
    )    
    return pos_response, neg_response


def push_team2_metric(pos_value, neg_value):
    cloudwatch = boto3.client('cloudwatch')
    neg_response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName' : 'Negative Sentiment',
                'Dimensions': [
                    {
                        'Name': 'Game_Sentiment',
                        'Value': 'Team2',
                    },
                ],
                'Unit': 'None',
                'Value': neg_value
            },
        ],
        Namespace = 'Sentiment'
    )

    pos_response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName' : 'Positive Sentiment',
                'Dimensions': [
                    {
                        'Name': 'Game_Sentiment',
                        'Value': 'Team2',
                    },
                ],
                'Unit': 'None',
                'Value': pos_value
            },
        ],
        Namespace = 'Sentiment'
    )    
    
    return pos_response, neg_response


def update_to_decimal(obj):
    return decimal.Decimal(obj)

def lambda_handler(event, context):
    comprehend = boto3.client('comprehend')

    logger.info(f"event {event}")
    tweet_id = event['Records'][0]['dynamodb']['NewImage']['id']['S']
    tweet_text = event['Records'][0]['dynamodb']['NewImage']['tweet_text']['S']
    try:
        tweet_hashtags = event['Records'][0]['dynamodb']['NewImage']['tweet_hashtags']['SS']
    except:
        tweet_hashtags = []
    team1_hashtags = event['Records'][0]['dynamodb']['NewImage']['team1_hashtags']['SS']    
    team2_hashtags = event['Records'][0]['dynamodb']['NewImage']['team2_hashtags']['SS']    

    sentiment = comprehend.detect_sentiment(Text=tweet_text,
                                                 LanguageCode='en')
    logger.info(f"sentiment {sentiment}")
    pos_sentiment = sentiment['SentimentScore']['Positive']
    pos_sentiment = update_to_decimal(pos_sentiment)

    neg_sentiment = sentiment['SentimentScore']['Negative']
    neg_sentiment = update_to_decimal(neg_sentiment)

    response = update_to_stream_dynamo(tweet_id, pos_sentiment, neg_sentiment)
    
    total_team1_hashtags = []
    for tag in tweet_hashtags:
        if any(tag in team1_hashtags for team1_hashtags in team1_hashtags):
            total_team1_hashtags.append(tag)
    logger.info(f'team1 hashtags: {total_team1_hashtags}')
    
    
    total_team2_hashtags = []
    for tag in tweet_hashtags:
        if any(tag in team2_hashtags for team2_hashtags in team2_hashtags):
            total_team2_hashtags.append(tag)
    logger.info(f'team2 hashtags: {total_team2_hashtags}')
            
    if len(total_team1_hashtags) > 0:
        logger.info("pushing team1 metric: {pos_sentiment}")
        push_team1_metric(pos_sentiment, neg_sentiment)

    if len(total_team2_hashtags) > 0:
        logger.info("pushing team2 metric: {pos_sentiment}")
        push_team2_metric(pos_sentiment, neg_sentiment)
    
        #metric_response = update_to_metric_dynamo(tweet_id, pos_sentiment, neg_sentiment)


    return True


