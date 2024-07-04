import botocore.exceptions
import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import botocore
import boto3
import json

#from database_utils import DataExtractor 

class DataExtractor:    
  
    def  read_rds_table(self,rds_engine, table_name):
      # credentials = db_connector.read_db_creds() 
      # engine = db_connector.init_db_engine(credentials)
       query = f"SELECT * FROM {table_name}"
       df = pd.read_sql(query,rds_engine)
       print(df)
       return df

    def retrieve_pdf_data(self, pdf_link):
        df = tabula.read_pdf(pdf_link,pages='all')
        combined_df = pd.concat(df, ignore_index=True)
        return combined_df

    def list_number_of_stores(self,no_stores_endpoint, headers):
        response = requests.get(no_stores_endpoint,headers = headers)
        #check if the request was successful
        if(response.status_code == 200):
            return response.json()['number_stores']
        #if the request was not successfull print status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
       

    def retrieve_stores_data(self, retreivestore_endpoint, headers, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores+1):
            get_url = retreivestore_endpoint.format(store_number=store_number)
            response = requests.get(get_url,headers=headers)
            if(response.status_code == 200):
                stores_data.append(response.json()) 
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        return pd.DataFrame(stores_data)
    
    def extract_from_s3(self, s3_url):
        try:
            s3 = boto3.client('s3')
            bucket, key = s3_url.replace("s3://", "").split("/", 1)
            s3_obj = s3.get_object(Bucket=bucket, Key=key)
           #print("s3_obj:",s3_obj)
            prod_df = pd.read_csv(s3_obj['Body'])
            print(prod_df['weight'])
            #data_top = prod_df.head()   
            return prod_df
      
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist")

    def extract_json_from_s3(self,s3_json_url):
        bucket_name = s3_json_url.split('/')[2].split('.')[0]
        file_path = '/'.join(s3_json_url.split('/')[3:])
        try:
            s3 = boto3.client('s3')
            s3_json_obj = s3.get_object(Bucket = bucket_name, Key = file_path)
            json_data = s3_json_obj['Body'].read().decode('utf-8')
            # Load JSON data
            json_data = json.loads(json_data)
            print(f"json_data:",json_data)
            # Convert JSON data to DataFrame
            json_data_df = pd.DataFrame(json_data)
            return json_data_df
            
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist")
         
        
           


