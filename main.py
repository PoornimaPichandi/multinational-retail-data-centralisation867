from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#api key
api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
headers = {'x-api-key': api_key}
#endpoint to retreive a store
retreivestore_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
#endpoint to retreive the number of stores
no_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

s3_url = 's3://data-handling-public/products.csv'
s3_json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

#read database tables

#read from rds database
rds_filepath = 'C:/Users/poorn/Documents/Python Scripts/git_repo/multinational-retail-data-centralisation867/db_creds.yaml'
creds = db_connector.read_db_creds(rds_filepath) 
#print(creds)
rds_engine = db_connector.init_db_engine(creds)
#read from local database
creds = db_connector.read_db_creds() 
local_engine = db_connector.init_db_engine(creds)

#list database tables
tables = db_connector.list_db_tables(rds_engine)


#Read user data
user_df = data_extractor.read_rds_table(rds_engine, 'legacy_users')
print("First few rows of the user DataFrame:\n", user_df.head())

# Extract orders data
orders_df = data_extractor.read_rds_table(rds_engine, 'orders_table')
print("First few rows of the orders DataFrame:\n", orders_df.head())

# Clean user data
cleaned_user_df = data_cleaning.clean_user_data(user_df)

# Cleaned orders data
cleaned_orders_df = data_cleaning.clean_orders_data(orders_df)
print("First few rows of the cleaned orders DataFrame:\n", cleaned_orders_df.head())

#upload dimusers to local database sales_data
db_connector.upload_to_db(cleaned_user_df, 'dim_users',local_engine)

#Read data from pdf
pdf_link = 'C:/Users/poorn/Documents/Python Scripts/git_repo/multinational-retail-data-centralisation867/card_details.pdf'
pdf_data_df = data_extractor.retrieve_pdf_data(pdf_link)
print("First few rows of the PDF DataFrame:\n", pdf_data_df.head())

#cleaned card data 
clean_card_data = data_cleaning.clean_card_data(pdf_data_df)
print("First few rows of cleaned:\n",clean_card_data.head())

#upload card details to local database 
db_connector.upload_to_db(clean_card_data,'dim_card_details',local_engine)

# List number of stores
number_of_stores = data_extractor.list_number_of_stores(no_stores_endpoint, headers)
print("Number of stores:", number_of_stores)

# Retreive store data
stores_df = data_extractor.retrieve_stores_data(retreivestore_endpoint,headers, number_of_stores)
print("First few rows of cleaned:\n",stores_df.head())

#clean store data
clean_store_data = data_cleaning.clean_store_data(stores_df)
print("First few rows of cleaned store data:\n",stores_df.head())

#upload store details to local database
db_connector.upload_to_db(clean_store_data,'dim_store_details',local_engine)

#Extract product data from s3
products_df = data_extractor.extract_from_s3(s3_url)
print("First few rows of product data frame:\n",products_df.head())

#Convert products weight
converted_products_df = data_cleaning.convert_product_weights(products_df)
print("First few rows of the products DataFrame with converted weights:\n", converted_products_df.head())

#clean products data
clean_products_data = data_cleaning.clean_products_data(converted_products_df)
print("First few rows of clean products data frame\n:",clean_products_data)

#upload products data
db_connector.upload_to_db(clean_products_data,'dim_products',local_engine)


# Upload cleaned orders data to the orders_table
db_connector.upload_to_db(cleaned_orders_df, 'orders_table',local_engine)

#extract json data from s3 url
date_details_df = data_extractor.extract_json_from_s3(s3_json_url)
print("First few rows of the date details DataFrame:\n", date_details_df.head())

#clean date details from json data
cleaned_date_details_df = data_cleaning.clean_date_details(date_details_df)
print("First few rows of the cleaned date details DataFrame:\n", cleaned_date_details_df.head())

#upload date details to local database
db_connector.upload_to_db(cleaned_date_details_df, 'dim_date_times',local_engine)