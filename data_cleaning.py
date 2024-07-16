import pandas as pd
import re
class DataCleaning:
    """
    A class that handles data cleaning operations
    """
    def clean_user_data(self,user_df):
        """
        This method used for cleaning data by handling null values and fixing data types
        """
        #Remove rows with NULL values
        print("Available columns in the DataFrame:", user_df.columns)
        
        # Identify the date column (assuming it's either 'join_date' or 'date_of_birth')
        date_column = None
        if 'join_date' in user_df.columns:
            date_column = 'join_date'
        elif 'date_of_birth' in user_df.columns:
            date_column = 'date_of_birth'
        else:
            raise KeyError("No valid date column found in DataFrame")

       # Convert the identified date column to datetime
        user_df[date_column] = pd.to_datetime(user_df[date_column], errors='coerce')
        user_df = user_df.dropna(subset=[date_column])

        # Additional cleaning logic (example: correcting data types)
        if 'int_column' in user_df.columns:
            user_df['int_column'] = user_df['int_column'].astype(int)
        if 'float_column' in user_df.columns:
            user_df['float_column'] = user_df['float_column'].astype(float)

        # Remove rows with incorrect information (example: assuming 'int_column' must be > 0)
        if 'int_column' in user_df.columns:
            user_df = user_df[user_df['int_column'] > 0]

        return user_df
    
    def clean_card_data(self, df):
        """
        This method cleans card data by handling null values and fixing data types.
        Args:
        df - Dataframe containing card data.
        Retunrs: returns dataframe Cleaned card dataframe 
        """
        # Drop rows with any missing values
        df = df.dropna()

        # Find the date column (adjust the column name as needed)
        possible_date_columns = ['expiry_date', 'date_payment_confirmed']
        date_column = None
        for col in possible_date_columns:
            if col in df.columns:
                date_column = col
                break

        if date_column:
            df[date_column] = pd.to_datetime(df[date_column], infer_datetime_format=True, errors='coerce')
            df = df.dropna(subset=[date_column])
        else:
            print("No valid date column found")

        # Find integer and float columns (adjust the column names as needed)
        possible_int_columns = ['int_column', 'user_uuid', 'phone_number']
        possible_float_columns = ['float_column', 'phone_number']

        for col in possible_int_columns:
            if col in df.columns:
                df[col] = df[col].astype(int)
                df = df[df[col] > 0]

        for col in possible_float_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)

        return df

    
    def clean_store_data(self, stores_df):  
        """
        Cleans store data by handling null values and fixing data types.
        Args: 
        store_df - Dataframe containing store data.
        Return : returns cleaned store data frame  
        """       
       # Replace 'NULL' strings with actual NaN values
        stores_df.replace('NULL', pd.NA, inplace=True)
    
        # Remove rows where 'address' contains 'NULL'
        stores_df = stores_df.dropna(subset=['address'])
    
        # Remove rows where 'longitude' cannot be converted to a float
        stores_df = stores_df[pd.to_numeric(stores_df['longitude'], errors='coerce').notnull()]
    
        # Ensure 'staff_numbers' is of type string to handle any non-numeric values
        stores_df['staff_numbers'] = stores_df['staff_numbers'].astype(str)
    
        # Keep rows where 'staff_numbers' can be converted to an integer
        stores_df = stores_df[stores_df['staff_numbers'].str.isdigit()]
    
        # Convert 'staff_numbers' back to integers
        stores_df['staff_numbers'] = stores_df['staff_numbers'].astype(int)
        # Correct entries in 'continent' column
        stores_df['continent'] = stores_df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})
    
        # Remove rows with any NaN values
        df_cleaned = stores_df.dropna()
    
        # Remove duplicate rows
        df_cleaned = df_cleaned.drop_duplicates()    
        return stores_df
   

    def convert_product_weights(self, products_df):   
        """Converts product weights to kilograms.

        Args:
            products_df : DataFrame containing product data.

        Returns:
            Returns DataFrame with converted weights
        """   
        def parse_weight(weight):
            weight = str(weight).lower().strip()
            # Define patterns for different formats
            kg_pattern = re.compile(r'(\d+\.?\d*)\s*kg')
            g_pattern = re.compile(r'(\d+\.?\d*)\s*g')
            ml_pattern = re.compile(r'(\d+\.?\d*)\s*ml')
            x_pattern = re.compile(r'(\d+)\s*x\s*(\d+)')
            # Check and convert different formats
            if kg_pattern.match(weight):
                return float(kg_pattern.match(weight).group(1))
            elif g_pattern.match(weight):
                return float(g_pattern.match(weight).group(1)) / 1000
            elif ml_pattern.match(weight):
                return float(ml_pattern.match(weight).group(1)) / 1000  # Assuming 1:1 ratio of ml to g
            elif x_pattern.match(weight):
                parts = x_pattern.findall(weight)
                if len(parts[0]) == 2:
                    return (float(parts[0][0]) * float(parts[0][1])) / 1000
            else:
                # Handle other cases by extracting the first number found
                numbers = re.findall(r'\d+\.?\d*', weight)
                if numbers:
                    return float(numbers[0]) / 1000  # Assume default to grams if no unit provided
                else:
                    return None

        # Apply the parse_weight function to the weight column
        products_df['weight'] = products_df['weight'].apply(parse_weight)
        products_df = products_df.dropna(subset=['weight'])
        return products_df

       
    def clean_products_data(self,products_df):
        # Function to check if product_price is valid
        def is_valid_price(price):
            try:
                float(price.replace('£', ''))
                return True
            except ValueError:
                return False
        # Apply the function to filter out invalid product prices
        products_df = products_df[products_df['product_price'].apply(is_valid_price)]
        # Remove the '£' character from the 'product_price' column and convert it to numeric
        products_df['product_price'] = products_df['product_price'].str.replace('£', '').astype(float, errors='ignore')
    
        # Ensure 'weight' column is numeric, handle non-numeric values
        products_df['weight'] = pd.to_numeric(products_df['weight'], errors='coerce')
        products_df = products_df.dropna(subset=['weight'])           

        # Remove rows with any NaN values
        products_df_cleaned = products_df.dropna()           
        # Remove duplicate rows
        products_df_cleaned = products_df_cleaned.drop_duplicates()
        #Reset index
        products_df_cleaned = products_df.reset_index(drop=True)  
        return products_df_cleaned
    
    def clean_orders_data(self, orders_df):
        # Drop first,lastname columns
        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'], errors='ignore')
        return orders_df
    
    def clean_date_details(self, json_df):        

        for column in ['year', 'month', 'day']:
            if column in json_df.columns:
                json_df[column] = pd.to_numeric(json_df[column], errors='coerce')
    
        # Remove rows with any NaN values in date-related columns
        json_df = json_df.dropna(subset=['year', 'month', 'day'])
    
        # Check for valid months (1 to 12)
        json_df = json_df[json_df['month'].between(1, 12)]
    
        # Remove rows with any NaN values
        json_df = json_df.drop(columns=['timestamp'])
        json_df.dropna()    
        # Reset the index
        json_df.reset_index(drop=True)
        # Ensure the 'date_uuid' is treated as a string
        json_df['date_uuid'] = json_df['date_uuid'].astype(str)
        # Reset the index
        json_df = json_df.reset_index(drop=True)
        return json_df