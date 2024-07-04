import pandas as pd
import re
class DataCleaning:
    def clean_user_data(self,user_df):
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

    
    def clean_store_data(self, store_df):        
        store_df = store_df.dropna()
        return store_df

    def convert_product_weights(self, products_df):        
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
        products_df = products_df.dropna(subset=['weight'])
        return products_df
    
    def clean_orders_data(self, orders_df):
        # Drop first,lastname columns
        orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'], errors='ignore')
        return orders_df
    
    def clean_date_details(self, json_df):        
        # Combine date-related fields into a single datetime column
        json_df['datetime'] = pd.to_datetime(json_df['year'].astype(str) + '-' + json_df['month'].astype(str) + '-' + json_df['day'].astype(str) + ' ' + json_df['timestamp'], errors='coerce')
        
        # Drop rows with NaT in 'datetime' column
        json_df = json_df.dropna(subset=['datetime'])
        
        # Drop the individual date-related columns if they are no longer needed
        json_df = json_df.drop(columns=['year', 'month', 'day', 'timestamp'])
         
        # Ensure the 'date_uuid' is treated as a string
        json_df['date_uuid'] = json_df['date_uuid'].astype(str)
        return json_df