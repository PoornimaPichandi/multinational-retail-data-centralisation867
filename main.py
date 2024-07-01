from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

#read database tables
creds = db_connector.read_db_creds('C:/Users/poorn/Documents/Python Scripts/git_repo/multinational-retail-data-centralisation867/db_creds.yaml') 
engine = db_connector.init_db_engine(creds)
#list database tables
tables = db_connector.list_db_tables(engine)

#Read user data
user_df = data_extractor.read_rds_table(db_connector, 'legacy_users')
print("First few rows of the user DataFrame:\n", user_df.head())

# Clean user data
cleaned_user_df = data_cleaning.clean_user_data(user_df)


db_connector.upload_to_db(cleaned_user_df, 'dim_users',creds)
