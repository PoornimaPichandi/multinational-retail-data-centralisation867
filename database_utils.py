import os
import pandas as pd
from sqlalchemy import create_engine, inspect
import yaml
'''
This Class is used to create database connection and operations using read_db_creds() method  
and initialize database engine using init_db_engine() method, this method will retreive the db credentials from read_db_creds method
when the database engine is initialized, creating a method list_db_tables() to list all the tables from the database. 

'''
class DatabaseConnector:         
    # filepath = 'C:/Users/poorn/Documents/Python Scripts/git_repo/multinational-retail-data-centralisation867/db_local_creds.yaml'
    # def __init__(self,filepath='db_local_creds.yaml'):
    #    self.filepath = filepath

    def read_db_creds(self,filepath='db_local_creds.yaml'):  
        """
        Read database credentials from a YAML file.

        Args: 
            filepath is the path to the YAML default class file path
        Returns: 
            dict: Database Credentials
        """ 
        # if filepath is None:
        #   # filepath = self.filepath

        if os.path.exists(filepath):
         with open(filepath,'r') as file:
            creds = yaml.load(file, Loader=yaml.FullLoader)
            print(type(creds))
        else:
           print(f'file not found:{filepath}')
        return creds  
        
    def init_db_engine(self,creds:dict):   
       """
       Initialize and returns a sqlAlchemy database engine.

       Args: 
       creds:dict --> database Credentials

       Returns:
        sqlalchemy.engine.Engine: Database Engine.

       """ 

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

       Args: engine(sqlAlchemy.engine.Engine): Database engine.

       Return : 
        list: List of table names
       '''
       inspector = inspect(engine)
       tables = inspector.get_table_names()
       print(tables)
       return tables
    
    def upload_to_db(self,df,table_name,engine):
        """
        Uploads a Dataframe to the Database.

        Args: 
        df(pd.DataFrame) : Dataframe to upload.
        table_name : Name of the table to upload data to.
        engine : Databse Credentials        
        """
       
        df.to_sql(table_name, engine, if_exists='replace')
       

  

