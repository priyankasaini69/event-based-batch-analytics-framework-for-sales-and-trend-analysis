from pyspark import SparkContext 
from operator import add
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import sum
from datetime import datetime
import boto3
import argparse
import os

# Create a Spark session
spark = SparkSession.builder.appName("AggregationJob").getOrCreate()

# import other required modules for naming the pyspark file
from pyspark.sql import DataFrameWriter

parser = argparse.ArgumentParser(description='Process some folder.')
parser.add_argument('-f', '--folder') 
args = parser.parse_args()

folder =  args.folder

s3_bucket = "s3://staging-and-validation/"
input_folder = "Validated-Data-Layer/Filtered-Data/DEL-Store/DEL-A/"
dest_bucket = "s3://aggregated-layer"
output_folder = "DEL-Store/DEL-A"

# Read the CSV files into a DataFrame
file1 = spark.read.csv(s3_bucket + input_folder + folder + "/" + "*.csv", header=True)

# Select only the relevant columns
selected_columns = ["franchise_store", "item", "transaction_date", "sale_amount", "tax_amount", "discount_amount"]
filtered1 = file1.select(*selected_columns)

# Perform the aggregation
aggregated_df = filtered1.groupBy("franchise_store", "item", "transaction_date").agg(
    sum("sale_amount").alias("total_sale_amount"),
    sum("tax_amount").alias("total_tax_amount"),
    sum("discount_amount").alias("total_discount_amount")
)

# Get the store ID
store_id = 'A-DEL'  # Replace with the desired store ID

# Update the DynamoDB table
dynamodb = boto3.resource('dynamodb', region_name = 'ap-south-1')
table = dynamodb.Table('Aggregation_update')

# Retrieve the item from the table based on the store ID
response = table.get_item(Key={'StoreId': store_id})
if 'Item' in response:
    item = response['Item']  # Retrieve the item from the response
else:
    # Item not found, create a new item with the store ID
    item = {'StoreId': store_id}
if folder in item:
    # Folder column exists, update the value to 'yes'
    item[folder] = 'yes'
else:
    # Folder column doesn't exist, add the column with value 'yes'
    item[folder] = 'yes'
table.put_item(Item=item)

# Generate the output file name based on the input file names
name = folder + ".csv"

final_file = os.path.join(dest_bucket,output_folder,folder)

# Save the aggregated DataFrame as a CSV file in S3
aggregated_df.write.csv(final_file, header= True, mode="overwrite")

spark.stop()