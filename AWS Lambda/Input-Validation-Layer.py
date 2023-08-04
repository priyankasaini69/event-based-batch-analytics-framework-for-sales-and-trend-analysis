import json
import urllib.parse
import boto3
import csv
import random
from datetime import datetime

def lambda_handler(event, context):
    # update the lambda_handler function to handle multiple files in s3 event
    s3 = boto3.client('s3')
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        source_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
    
        # Extracting date and store name from the CSV file name
        file_name_parts = source_key.split('/')

        # Destination bucket and folder for filtered and bad data
        filtered_bucket = 'staging-and-validation'
        bad_data_bucket = 'staging-and-validation'
        filtered_folder = 'Validated-Data-Layer/Filtered-Data/DEL-Store/DEL-A/'
        bad_data_folder = 'Validated-Data-Layer/Bad-Data/DEL-Store/DEL-A/'
        
        # Read CSV file from S3
        response = s3.get_object(Bucket=source_bucket, Key=source_key)
        lines = response['Body'].read().decode('utf-8').split('\n')
        csv_reader = csv.DictReader(lines)
        
        # Initialize lists for filtered and bad data
        filtered_data = []
        bad_data = []
        
        # Apply filters to the dataset
        for row in csv_reader:
            if validate_row(row):
                filtered_data.append(row)
            else:
                bad_data.append(row)
        
        # Generate filtered data CSV file
        first_part = file_name_parts[-1]
        first_part_splits = first_part.split('_')
        file_name_parts_one = first_part_splits[0]

        # FILTERED FILE
        filtered_file_name = first_part.replace('.csv', '_Filter.csv')
        filtered_file_path = filtered_folder + file_name_parts_one + '/' + filtered_file_name
        write_csv_to_s3(filtered_data, filtered_bucket, filtered_file_path)
        
        # BAD DATA FILE
        bad_data_file_name = first_part.replace('.csv', '_Bad-Data.csv')
        bad_data_file_path = bad_data_folder + file_name_parts_one + '/'  + bad_data_file_name
        write_csv_to_s3(bad_data, bad_data_bucket, bad_data_file_path)

def validate_row(row):
    # Check for various filtering rules
    if row['franchise_state'].strip() == 'null':
        return False
    if row['segment'].strip() == 'null':
        return False
    if row['ship_mode'].strip() == 'null':
        return False
    if row['transaction_date'].strip() == 'null':
        return False
    if row['delivery_city'].strip() == 'null':
        return False
    if row['order_id'].strip() == 'null':
        return False
    
    # Check for date format
    try:
        datetime.strptime(row['order_date'], '%Y-%m-%d')
    except ValueError:
        return False

    # Check date format for 'ship_date'
    try:
        datetime.strptime(row['ship_date'], '%Y-%m-%d')
    except ValueError:
        return False

    # Check date format for 'transaction_date'
    try:
        datetime.strptime(row['transaction_date'], '%Y-%m-%d')
    except ValueError:
        return False
        
    # Check for negative values
    if int(row['quantity']) < 0 or float(row['sale_amount']) < 0 or float(row['tax_amount']) < 0:
        return False

    return True

def write_csv_to_s3(data, bucket, key):
    #Extract the header from the data
    header = ','.join(data[0].keys())

    # Write the data to a CSV file
    csv_data = '\n'.join([','.join(row.values()) for row in data])

    #Prepend the header to the csv data
    csv_data = header + '\n' + csv_data
    
    # Upload the CSV file to S3
    s3 = boto3.client('s3')
    s3.put_object(Body=csv_data.encode('utf-8'), Bucket=bucket, Key=key)