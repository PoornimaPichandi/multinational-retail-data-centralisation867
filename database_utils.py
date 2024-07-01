import yaml
import os
from sqlalchemy import create_engine, inspect
#import data_extraction
import pandas as pd
'''
This Class is used to create database connection using read_db_creds() method  
and initialize database engine using init_db_engine() method, this method will retreive the db credentials from read_db_creds method
when the database engine is initialized, creating a method list_db_tables() to list all the tables from the database. 

'''
class DatabaseConnector:         
    filepath = 'C:/Users/poorn/Documents/Python Scripts/git_repo/multinational-retail-data-centralisation867/db_local_creds.yaml'
    def __init__(self,filepath='db_local_creds.yaml'):
       self.filepath = filepath

    def read_db_creds(self,filepath=None):   
        if filepath is None:
           filepath = self.filepath

        if os.path.exists(filepath):
         with open(filepath,'r') as file:
            creds = yaml.load(file, Loader=yaml.FullLoader)
            print(type(creds))
        else:
           print(f'file not found:{self.filepath}')
        return creds  
        
    def init_db_engine(self,creds:dict):    

       #database_url = (f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}"
       #                  f"@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
       database_url = (f"postgresql://{creds['USER']}:{creds['PASSWORD']}"
                       f"@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
       #engine = create_engine(f"{creds['DATABASE_TYPE']}+{creds['DBAPI']}://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
       engine = create_engine(database_url)
       print(engine)
       return engine
    
    def list_db_tables(self,engine):
       '''
       List all tables in the database using SQLAlchemy engine
       '''
       inspector = inspect(engine)
       tables = inspector.get_table_names()
       print(tables)
       return tables
    
    def upload_to_db(self,df,table_name,creds):
        engine = self.init_db_engine(creds) 
        df.to_sql(table_name,engine,if_exists='replace',index=False)
       



