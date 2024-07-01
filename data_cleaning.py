import pandas as pd
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
    
    
