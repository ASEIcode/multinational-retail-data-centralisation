from data_extraction import DataExtractor as de
import pandas as pd
import numpy as np
import re

class DataCleaning:
    """
    Contains methods for cleaning pandas dataframes taken in from the DataExtractor class.
    """
    def __init__(self) -> None:
        self.extractor = de() # create an instance of the DataExtractor class
    def clean_user_data(self):
        """ Extracts data using DataExtractor and cleans the following:
                - String "Null" in place of null values (Rows Dropped).
                - Incorrect names with numbers and capital letter combos dropped (has_numbers)
                - date_of_birth & join_date changed to datetime types
                - string columns changed to lower case to avoid search errors
                - country_code error fixed GBB to GB
                - country_code and country changed to category type
                - Removes escape characters and slashes from address and replace with comma space
                - Drops the extra index column and resets the index
            Returns -> user_data
        """
        user_data = self.extractor.read_rds_table("legacy_users")

        user_data = user_data[user_data['first_name'] != 'NULL'] 

        def has_numbers(input_string):
            "Looks for numbers in a string"
            return any(char.isdigit() for char in input_string)
        user_data = user_data[~user_data['first_name'].apply(has_numbers)]

        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format = 'mixed')
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], format = 'mixed')

        user_data['first_name'] = user_data['first_name'].apply(lambda x:x.lower())
        user_data['last_name'] = user_data['last_name'].apply(lambda x:x.lower())
        user_data['company'] = user_data['company'].apply(lambda x:x.lower())
        user_data['email_address'] = user_data['email_address'].apply(lambda x:x.lower())
        user_data['address'] = user_data['address'].apply(lambda x:x.lower())

        user_data['country'] = user_data['country'].astype('category')
        user_data['country_code'] = user_data['country_code'].apply(lambda x: x.replace('GGB', 'GB'))
        user_data['country_code'] = user_data['country_code'].astype('category')
        

        user_data['address'] = user_data['address'].apply(lambda x: x.replace("\n", ", "))
        user_data['address'] = user_data['address'].apply(lambda x: x.replace("/", ""))
        user_data['address'] = user_data['address'].apply(lambda x: x.lower())
        user_data.drop("index", axis=1, inplace=True)
        user_data.reset_index(inplace=True, drop=True)  

        return user_data
    
    def clean_card_data(self):
        """Extracts card data from a PDF in Amazon S3 Bucket specified in the URL and cleans the following:
                - drops rows with null values
                - changes card_provider to category dtype
                - removes '???' from card numbers
                - drops leftover rows which are not numeric
                - changes card_number to int64 dtype
                - adds a day to the the expiry date and changes it to a datetime64[ns] dtype
                - changes date_payment confirmed to a datetime64[ns] dtype
                - resets the index dropping the original
            returns -> card_data
        """
        card_data = self.extractor.retrieve_pdf_data()
        card_data.dropna(inplace=True)
        card_data['card_provider'] = card_data['card_provider'].astype('category')

        card_data['card_number'] = card_data['card_number'].apply(lambda x: x.replace('?', '')) #  1. removes ??? from card numbers  first
        card_data = card_data[card_data['card_number'].str.isnumeric()] # 2. then drops non numeric - must be in this order
        
        card_data['card_number'] = card_data['card_number'].astype('int64')
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], dayfirst=True, format='mixed')

        card_data.reset_index(inplace=True, drop=True) # should go at the end of all cleaning

        return card_data
    
    def clean_store_data(self):
        """
        Cleans the store data by performing the following actions:
            - Drop lat column. Has many missing and erroneous values. Data not useful.
            - Drop all erroneous rows with capital letters and numbers as single words e.g. 9D4LK7X4LZ as an address
            - Drop 3 rows with string 'NULL'.
            - Correct ee prefix in continent column entries (eeAmerica > America) and set as category type
            - Opening Date changed to datetime type
            - Clean up errors in longitude/latitude column and convert to Float64
            - Remove extra alpha characters around the numbers in the staff numbers col and change type to Int64
            - Change store_type and country_code to category dtype
            - Replace slashes and new line escape chars in address with ", "
            - Drop the current index col and reset the index
        Returns -> df_store
        """
        df_store = self.extractor.retrieve_stores_data()
        df_store.drop('lat', axis=1, inplace=True)
        
        def has_numbers(input_string):
            "Looks for numbers in a string"
            return any(char.isdigit() for char in input_string)
        df_store = df_store[~df_store['continent'].apply(has_numbers)]

        df_store = df_store[df_store['address'] != 'NULL']

        df_store.loc[:, 'continent'] = df_store['continent'].apply(lambda x: x.replace("eeEurope", "Europe"))
        df_store.loc[:, 'continent'] = df_store['continent'].apply(lambda x: x.replace("eeAmerica", "America"))
        df_store['continent'] = df_store['continent'].astype('category')

        df_store['opening_date'] = pd.to_datetime(df_store['opening_date'], format='mixed')

        df_store.loc[0,'longitude'] = np.nan
        df_store.loc[0,'latitude'] = np.nan
        df_store['longitude'] = df_store['longitude'].astype('Float64')
        df_store['latitude'] = df_store['latitude'].astype('Float64')

        df_store['staff_numbers'] = df_store['staff_numbers'].apply(lambda x: re.sub(r'\D', '', x))
        df_store['staff_numbers'] = df_store['staff_numbers'].astype('int64')

        df_store['store_type'] = df_store['store_type'].astype('category')
        df_store['country_code'] = df_store['country_code'].astype('category')

        df_store['address'] = df_store['address'].apply(lambda x: x.replace("\n", ", "))
        df_store['address'] = df_store['address'].apply(lambda x: x.replace("/", ""))

        df_store.reset_index(inplace=True, drop=True)
        df_store.drop('index', axis=1, inplace=True )

        return df_store
    
    def convert_product_weights(self, products_df):
        """
        Takes the products dataframe as an argument and uses regex conditional statements
        to check the units and apply the correct conversion to kgs and remove unit name.
        Returns corrected value to the dataframe and then returns the dataframe itself.
        """

        def convert_to_kg(inputstring):
            """"
            Uses regex to search for various unit patterns and errors.
            Converts each value to kilograms and removes the unit name.
            Returns > products_df
            """
        
            pattern1 = r"^[0-9]+kg$" # ?kgs
            pattern2 = r"^[0-9]+ml$" # ?mls
            pattern3 = r"^[0-9]+kg .$" # ?kg .
            pattern4 = r"^[0-9]+oz$" # ?oz
            pattern5 = r"^[0-9]+g$" # ?g
            pattern6 = r"^[0-9]+ x [0-9]+kg$" # ? x ?kg
            pattern7 = r"^[+-]?\d+(\.\d+)?g$" # grams float
            pattern8 = r"^[+-]?\d+(\.\d+)?kg$"# kg as a float
            pattern9 = r"^[0-9]+ x [0-9]+g$" # ? x ?g
            pattern10 = r"^[0-9]+g .$" # ?g .

            if re.match(pattern1, inputstring): # ?kgs
                return inputstring.replace("kg", "")
            elif re.match(pattern2, inputstring): # ?mls
                inputstring.replace("ml", "")
                ml_to_kilos = inputstring.replace("ml", "")
                return float(ml_to_kilos) / 1000
            elif re.match(pattern3, inputstring): # ?kg .
                return inputstring.replace("kg .", "")
            elif re.match(pattern4, inputstring): # ?oz
                oz_to_kilos = inputstring.replace("oz", "")
                return float(oz_to_kilos) / 35.274
            elif re.match(pattern5, inputstring): # ?g
                grams_to_kilos = inputstring.replace("g", "")
                return float(grams_to_kilos) / 1000
            elif re.match(pattern6, inputstring): # ? x ?kg
                num_stripped = inputstring.replace("kg", "")
                num_a = float(num_stripped.split(" x ")[0])
                num_b = float(num_stripped.split(" x ")[1])
                return num_a * num_b
            elif re.match(pattern7, inputstring): # grams float
                grams_to_kilos = inputstring.replace("g", "")
                return float(grams_to_kilos) / 1000
            elif re.match(pattern8, inputstring): # kg as a float
                return inputstring.replace("kg", "")
            elif re.match(pattern9, inputstring): # ? x ?g
                num_stripped = inputstring.replace("g", "")
                num_a = float(num_stripped.split(" x ")[0])
                num_b = float(num_stripped.split(" x ")[1])
                num_c = num_a * num_b
                return num_c / 1000
            elif re.match(pattern10, inputstring): # ?g .
                grams_to_kilos = inputstring.replace("g .", "")
                return float(grams_to_kilos) / 1000
            else:
                pass

        products_df['weight'] = products_df['weight'].apply(lambda x: convert_to_kg(str(x)))

        return products_df
    
    def clean_products_data(self):
        """
        Performs the following cleaning actions on the products data and returns the clean dataframe:
            - Drop old index column
            - Drop 3 erroneous rows with values like 123GSDFHH255
            - Convert all weights to Kg and rename weights column to 'weight_in_kg'
            - Change category and removed to category dtype
            - Remove the £ sign from the price col and change to float dtype
            - Change dtype of weight col to float
            - Drop null rows
            - Correct the format of dates not in standard format and change date_added to datetime dtype
            - reset index
        """
        df = self.extractor.extract_from_s3()
        products_df = self.convert_product_weights(df)

        products_df.drop('Unnamed: 0', axis=1, inplace=True)
        products_df.drop([751, 1400, 1133], inplace=True)

        products_df['category'] = products_df['category'].astype('category')
        products_df['removed'] = products_df['removed'].astype('category')

        products_df['product_price'] = products_df['product_price'].apply(lambda x: str(x).replace("£", ""))
        products_df['product_price'] = products_df['product_price'].astype('float64')

        products_df['weight'] = products_df['weight'].astype('Float64')
        products_df.rename(columns={'weight' : 'weight_in_kg'}, inplace=True)

        products_df.dropna(inplace=True)

        products_df.loc[307, 'date_added'] = '2018-10-22'
        products_df.loc[1217, 'date_added'] = '2017-09-06'
        products_df['date_added'] = pd.to_datetime(products_df['date_added'])

        products_df.reset_index(inplace=True, drop=True)

        return products_df
    
    def clean_orders_data(self):
        """
        Drops 'first_name', 'last_name', '1', 'level_0', 'index' columns from the dataframe.
        Returns > orders_data
        """
        orders_data = self.extractor.read_rds_table("orders_table")
        orders_data.drop(['first_name', 'last_name', '1', 'level_0', 'index'], axis=1, inplace=True)
        return orders_data
    
    def clean_events_data(self):
        """
        Performs the following cleaning operations on the dataframe:
            - Searches for letters in the timestamp column and removes the erroneous rows 
            with values like FGG727HA
            - changes the year, month and day columns to int dtypes
            - changes the time period to category dtype
            - resets the index
        Returns > events_data
        """
        events_data = self.extractor.extract_json_from_link()

        def has_alpha(input_string):
            "Looks for letters in a string"
            return any(char.isalpha() for char in input_string)
        events_data = events_data[~events_data['timestamp'].apply(has_alpha)]

        events_data['year'] = events_data['year'].astype('int64')
        events_data['month'] = events_data['month'].astype('int64')
        events_data['day'] = events_data['day'].astype('int64')
        events_data['time_period'] = events_data['time_period'].astype('category')
        events_data.reset_index(inplace=True, drop=True)

        return events_data