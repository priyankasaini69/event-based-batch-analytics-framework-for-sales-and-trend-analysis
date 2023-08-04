import json
import urllib.parse
import boto3
import csv
from datetime import datetime

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    dynamodb = boto3.client('dynamodb')
    stepfunctions = boto3.client('stepfunctions')
    
    # Extract bucket and key information from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    # Extract the folder name and filename from the key
    folder, state_name, filename, folder1  = extract_folder_and_filename(key)

    # Check if the folder name corresponds to the date of the specific day
    if folder == get_specific_day_folder():
        
        # Check if all three files have arrived in the folder
        if check_all_files_arrived(s3, bucket, folder1, state_name, key, filename):
            
            # Update the DynamoDB table
            update_dynamodb(dynamodb, folder, state_name)
            #Initialize Step Functions
            response = stepfunctions.start_execution(
            stateMachineArn='arn:aws:states:ap-south-1:251784258286:stateMachine:MyStateMachine',
            input=json.dumps({
                        "createCluster": 1,
                        "cluster": {
                            "ClusterId": "j-17DQ1TK2TF3IB"
                        },
                        "folder": folder
                        
                })  # Provide any input data for your workflow if needed
            )
            
    return {
        'statusCode': 200,
        'body': 'Process completed successfully.'
    }

def extract_folder_and_filename(key):
    # Extract the folder name and filename from the specific file name
    filename_parts = key.rsplit('_', 2)
    folder1 = filename_parts[0].rsplit('/', 1)[1]
    folder = filename_parts[0].rsplit('/', 1)[1].rsplit('_')[0]
    state_name = filename_parts[1]
    filename = filename_parts[2]
    return folder, state_name, filename, folder1
    

def get_specific_day_folder():
    # Get the folder name based on the date of the specific day
    specific_day = datetime.now().strftime('%Y-%m-%d')  # Get the current date
    specific_day_folder = specific_day + '_DEL-A'  # Folder name format: <specific_day>_DEL-A
    return specific_day

def check_all_files_arrived(s3, bucket, folder1, state_name, key, filename):
    # Check if all three files have arrived in the folder
    expected_files = [folder1 + '_' + state_name + '_' + filename, folder1 + '_' + state_name + '_' + filename, folder1 + '_' + state_name + '_' + filename]
    response = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    files = []

    if 'Contents' in response:
        files = [obj['Key'].split('/')[5] for obj in response['Contents']]
        update_flag = all(file in files for file in expected_files)
        return update_flag

def update_dynamodb(dynamodb, folder, state_name):
    # Update the DynamoDB table
    store_id = 'A-DEL'
    response = dynamodb.describe_table(TableName='FileInfo')
    table_description = response['Table']

    # Check if the folder column already exists in the table
    if folder in table_description['AttributeDefinitions']:
        update_expression = f'SET {folder} = :Flag'
        expression_attribute_values = {':Flag': {'S': 'YES'}}
    else:
        update_expression = f'SET #folder = :Flag'
        expression_attribute_values = {':Flag': {'S': 'YES'}}

    dynamodb.update_item(
        TableName='FileInfo',
        Key={
            'FileName': {'S': state_name},
            'StoreID': {'S': store_id}
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames={'#folder': folder},
        ExpressionAttributeValues=expression_attribute_values
    )