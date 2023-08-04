# Generate random datasetimport boto3
import boto3
from faker import Faker
import csv
import random
from datetime import datetime

def lambda_handler(event, context):
    # Set up S3 client
    s3 = boto3.client('s3')
    
    # Generate random dataset
    fake = Faker()
    def generate_dataset(num_rows):
        dataset = []
        dataset.append([
                'row_id', 'order_id', 'order_date', 'ship_date', 'ship_mode', 'segment', 'customer_id', 'customer_name', 'item', 'sale_amount',
                'tax_amount', 'quantity', 'discount_amount', 'profit', 'franchise_store', 'transaction_date',
                'delivery_city', 'franchise_state', 'postal_code', 'region', 'product_id', 'category', 'sub_category'
            ])
        items = [
            'cutting board', 'knife', 'frying pan', 'pot', 'saucepan', 'baking sheet', 'mixing bowl',
            'whisk', 'spatula', 'tongs', 'ladle', 'peeler', 'grater', 'measuring cups', 'measuring spoons',
            'colander', 'can opener', 'corkscrew', 'pepper mill', 'bottle opener', 'oven mitts',
            'food storage containers', 'baking dish', 'rolling pin', 'pastry brush', 'oven thermometer',
            'grill brush', 'kitchen scale', 'strainer', 'potato masher', 'garlic press', 'kitchen scissors',
            'tea kettle', 'casserole dish', 'tongs', 'salad spinner', 'ice cream scoop', 'egg slicer',
            'basting brush', 'pizza cutter', 'canister set', 'cutlery tray', 'butter dish',
            'salt and pepper shakers', 'whisk', 'paring knife', 'utility knife', 'chef\'s knife',
            'bread knife', 'steak knives', 'peeler', 'grater', 'bottle opener', 'wine opener', 'corkscrew',
            'measuring cups', 'measuring spoons', 'kitchen shears', 'thermometer', 'timer', 'whisk',
            'spatula', 'ladle', 'tongs', 'potato masher', 'pepper mill', 'cutting board', 'mixing bowls',
            'strainer', 'colander', 'saucepan', 'frying pan', 'roasting pan', 'baking dish',
            'casserole dish', 'grill pan', 'wok', 'muffin tin', 'cake pan', 'cookie sheet',
            'pastry brush', 'rolling pin', 'oven mitts', 'apron', 'dish rack', 'dishwashing gloves',
            'food storage containers', 'kitchen timer', 'oven thermometer', 'tongs', 'spatula', 'ladle',
            'whisk', 'peeler', 'grater', 'bottle opener', 'can opener', 'corkscrew', 'measuring cups',
            'measuring spoons', 'strainer', 'potato masher', 'garlic press', 'kitchen shears',
            'salad spinner', 'ice cream scoop', 'pizza cutter', 'basting brush', 'grill brush',
            'tea kettle', 'toaster', 'blender', 'food processor', 'coffee maker', 'microwave',
            'slow cooker', 'electric kettle', 'mixer', 'juicer', 'rice cooker', 'immersion blender',
            'hand mixer', 'waffle maker', 'electric griddle', 'electric skillet', 'pressure cooker',
            'air fryer', 'sous vide machine', 'toaster oven', 'bread machine'
        ]
        segments = ['Preparation', 'Cooking', 'Baking', 'Utensils', 'Appliances', 'Miscellaneous']
        franchise_stores = ['A', 'B', 'C']
        franchise_states = ['DEL','BLR','BOM','HYD']
        regions = ['East', 'West', 'North', 'South']
        cities = ['Andhra Pradesh', 'Arunachal Pradesh',
        'Assam', 'Bihar', 'Chhattisgarh', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh',
        'Jharkhand', 'Karnataka', 'Kerala', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya',
        'Mizoram', 'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana',
        'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal', 'Andaman and Nicobar Islands', 'Chandigarh',
        'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Lakshadweep', 'Puducherry']
        categories = ['Category1', 'Category2', 'Category3']
        sub_categories = ['SC1', 'SC2', 'SC3']

        for _ in range(num_rows):
            row_id = fake.random_int(min=1, max=1000000)
            order_id = fake.random_int(min=1, max=1000000)
            order_date = datetime.now().strftime('%Y-%m-%d')
            ship_date = datetime.now().strftime('%Y-%m-%d')
            ship_mode = fake.random_element(elements=('Standard', 'Express', 'Next Day', 'Two-Day', 'Ground', 'Air', 'Courier'))
            segment = fake.random_element(elements=segments)
            customer_id = fake.random_int(min=1, max=10000)
            customer_name = fake.name()
            item = fake.random_element(elements=items)
            sale_amount = fake.random_int(min=100, max=1000)
            tax_amount = fake.random_int(min=10, max=100)
            quantity = fake.random_int(min=1, max=10)
            discount_amount= fake.random_int(min=0, max=10)
            profit = fake.random_int(min=10, max=100)
            franchise_store = fake.random_element(elements=franchise_stores)
            transaction_date = datetime.now().strftime('%Y-%m-%d')
            delivery_city = fake.random_element(elements=cities)
            franchise_state = fake.random_element(elements=franchise_states)
            postal_code = fake.random_int(min=10000, max=99999)
            region = fake.random_element(elements=regions)
            product_id = fake.random_int(min=1, max=1000)
            category = fake.random_element(elements=categories)
            sub_category = fake.random_element(elements=sub_categories)

            # Introduce anomalies
            if random.random() < 0.1:
                #to make null rows
                delivery_city = ""
                franchise_state = ""
                segment = ""
                transaction_date = ""
                order_id = ""
                ship_mode = ""

            if random.random() < 0.2:
                #to change date format
                order_date = datetime.now().strftime('%d-%m-%Y')
                ship_date = datetime.now().strftime('%d-%m-%Y')

                #to add negative values
                quantity = -quantity
                profit = -profit
                row_id = -row_id
                
                
            dataset.append([
                row_id, order_id, order_date, ship_date, ship_mode, segment, customer_id, customer_name, item, sale_amount,
                tax_amount, quantity, discount_amount, profit, franchise_store, transaction_date,
                delivery_city, franchise_state, postal_code, region, product_id, category, sub_category
            ])

        return dataset

    # Generate the dataset
    num_rows = 1000 # Change this to the desired number of rows
    dataset = generate_dataset(num_rows)
    today = datetime.now().strftime('%Y-%m-%d')

    # Save the dataset as a CSV file
    files = ['F1', 'F2', 'F3']
    for file in files:
        print('hello')
        franchise_state = 'DEL-A'
        filename = f"{today}_{franchise_state}_{file}.csv"
        file_path = f"/tmp/{filename}"  # Temporary local file path
        dest_path = f"Data-Staging-Layer/DEL-Store/DEL-A/{today}/{filename}"
    
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(dataset)
    
        # Upload the file to S3 bucket
        bucket_name = 'staging-and-validation'  # Change this to your S3 bucket name
        s3.upload_file(file_path, bucket_name, dest_path)  # Change 'folder' to the desired folder name in the bucket
    
    
    return {
        'statusCode': 200,
        'body': 'Process completed successfully.',
        'date is': datetime.now().strftime('%Y-%m-%d')
    }
    
