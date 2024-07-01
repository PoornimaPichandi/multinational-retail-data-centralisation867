import pandas as pd
from database_utils import DatabaseConnector
#from database_utils import DataExtractor 

class DataExtractor:
    
    def  read_rds_table(self,db_connector: DatabaseConnector, table_name):
       credentials = db_connector.read_db_creds() 
       engine = db_connector.init_db_engine(credentials)
       query = f"SELECT * FROM {table_name}"
       df = pd.read_sql(query,engine)
       print(df)
       return df






