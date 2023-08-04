import json
import boto3
import csv
from datetime import datetime

def lambda_handler(event, context):
    # s3 = boto3.client('s3')
    dynamodb = boto3.client('dynamodb')    

    folder =  event["folder"]

    # Update the DynamoDB table
    update_dynamodb(dynamodb, folder)
    return {
        'statusCode': 200,
        'body': 'Process completed successfully.'
    }


def update_dynamodb(dynamodb, folder):
    # Update the DynamoDB table
    store_id = 'A-DEL'
    response = dynamodb.describe_table(TableName='Aggregation_update')
    table_description = response['Table']

    # Check if the folder column already exists in the table
    if folder in table_description['AttributeDefinitions']:
        update_expression = f'SET {folder} = :Flag'
        expression_attribute_values = {':Flag': {'S': 'NO'}}
    else:
        update_expression = f'SET #folder = :Flag'
        expression_attribute_values = {':Flag': {'S': 'NO'}}

    dynamodb.update_item(
        TableName='Aggregation_update',
        Key={
            'StoreId': {'S': store_id}
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames={'#folder': folder},
        ExpressionAttributeValues=expression_attribute_values
    )