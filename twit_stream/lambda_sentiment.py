import boto3
import json
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

def update_to_metric_dynamo(tweet_id, pos_tweet_sentiment, neg_tweet_sentiment):
    client = boto3.resource('dynamodb')
    table = client.Table('metric_summary')
    response = table.get_item(
        Key={
            'metric_name': 'total_row'
        }
    )

    logger.info(f'response {response}')

    try:
        new_tot_neg = response['Item']['total_neg'] + neg_tweet_sentiment
        new_tot_pos = response['Item']['total_pos'] + pos_tweet_sentiment
        new_tot_count = response['Item']['total_count'] + 1
        new_avg_neg = new_tot_neg / new_tot_count
        new_avg_pos = new_tot_pos / new_tot_count
        logger.info('UpdatingRow')
        update_response = table.update_item(
            Key={
                'metric_name': 'total_row'
            },
            UpdateExpression='set total_neg = :neg_value, total_pos = :pos_value, total_count = :total_count, avg_neg = :avg_neg_value, avg_pos = :avg_pos_value',
            ExpressionAttributeValues={
                ':neg_value': new_tot_neg,
                ':pos_value': new_tot_pos,
                ':total_count': new_tot_count,
                ':avg_neg_value': new_avg_neg,
                ':avg_pos_value': new_avg_pos
            }
        )
        response = push_metric(new_avg_pos, new_avg_neg)

    except:
        logger.info('Adding first row')
        put_response = table.put_item(
            Item={
                'metric_name': 'total_row',
                'total_neg':neg_tweet_sentiment,
                'total_pos':pos_tweet_sentiment,
                'total_count': 1,
                'avg_neg': neg_tweet_sentiment / 1,
                'avg_pos': pos_tweet_sentiment / 1
            }
        )


def push_metric(pos_value, neg_value):
    cloudwatch = boto3.client('cloudwatch')
    response = cloudwatch.put_metric_data(
        MetricData = [
            {
                'MetricName' : 'Positive Sentiment',
                'Dimensions': [
                    {
                        'Name': 'Test1',
                        'Value': 'Test2',
                    },
                ],
                'Unit': 'None',
                'Value': pos_value
            },
        ],
        Namespace = 'PositiveSentiment'
    )

    return response

def push_team1_metric(pos_value, neg_value):
    cloudwatch = boto3.client('cloudwatch')
    response = cloudwatch.put_metric_data(
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

    return response


def push_team2_metric(pos_value, neg_value):
    cloudwatch = boto3.client('cloudwatch')
    response = cloudwatch.put_metric_data(
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
        Namespace = 'PositiveSentiment'
    )

    return response


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
    
    
    
    metric_response = update_to_metric_dynamo(tweet_id, pos_sentiment, neg_sentiment)


    return True


