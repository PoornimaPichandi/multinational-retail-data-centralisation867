# Multinational Retail Data Centralisation:
    - multinational-retail-data-centralisation867

# Table of Contents:
-  Project Aim
-  Software Requirements
-  Prerequisites
-  Usage Instructions
   -  Milestone1
   -  Milestone 2
   -  Milestone 3
   -  Milestone 4
-  File Structure of this Project 
-  License Information

# Project Aim:
    In this Project, the sales data was scattered across different sources and the aim of this project is to centralize the data and store it in a database, serving a single source of sales data.

# Software Requirements:
   - VSCode
   - Postgres SQL
   - AWS
        1. RDS
        2. Postgresql
        3. S3 
   - 

# Prerequisites:
    To achieve the aim of this project learned and utilized AWS, SQL, Data cleaning with Pandas and Web APIs.
  - Required Packages in Python 
    - pip install pyYAML
    - pip install tabula-py
    - sqlalchemy
    - pip install pandas
    - pip install requests
    - pip install boto3
    - pip install botocore
    
## Usage Instructions
   ## Milestone 1 
   The github repository setup - created and cloned
   ## Milstone 2
- Extract and clean the data from the data sources
    - Setup a new database to store the data.
    - Initializing three project classes.
        - Data Extractor in data_extraction.py
        - DataBase utils in database_utils.py
        - Data cleaning in data_cleaning.py
     - Extract and clean the user data
  
    - Extracting users and cleaning card details.
    - Extract and clean the details of each store.
    - Extract and clean the product details.
    - Retreive and clean the orders table.
    - Retreive and clean the date events data.
  ## Milestone 3
  - Create the database schema
    - Cast the columns of the orders_table to the correct data types.
    - Cast the columns of the dim_users to the correct database.
    - Update dim_store_details table
    - Make changes to the dim_products table for the delivery team
    - update the dim_products table with the required data types.
    - Update the dim_date_times table.
    - Update the dim_card_details table.
    - Create the primary keys in the dimension table.
    - Finalizing the star based schema and add foreign keys to the orders_table.
    
  ## Milstone 4
  - Querying the data.
  - Answering the business questions and extracting the data from the database using SQL.    
   
## File Structure of the Project
    --multinational-retail-data-centralisation867
        |.gitignore
        |card_details.pdf        
        |data_cleaning.py
        |data_extraction.py
        |database_utils.py
        |db_creds.yaml.py
        |db_local_creds.yaml.py
        |main.py
        |products.csv
        |mrdc_data_centralization.sql
        |mrdc_ERD
        |mrdc_project_star_schema.sql
        |README.md


## License Information
    This project is licensed under the [NAME HERE] License - see the LICENSE.md file for details

