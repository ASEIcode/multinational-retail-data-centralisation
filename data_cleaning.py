from data_extraction import DataExtractor as de
from tabula import read_pdf
import pandas as pd

class DataCleaning:
    def __init__(self) -> None:
        pass
    def clean_user_data(self):
        """ Extracts data using DataExtractor and cleans the following:
                - String "Null" in place of null values (Rows Dropped).
                - Incorrect names with numbers and capital letter combos dropped (has_numbers)
                - date_of_birth & join_date changed to datetime types
                - string columns changed to lower case to avoid search errors
                - country_code error fixed GBB to GB
                - country_code and country changed to category type
                - Removes escape characters and slashes from address and replace with comma space
            Returns -> user_data
        """
        extractor = de()
        user_data = extractor.read_rds_table("legacy_users") # extract the user data to a data frame

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
        user_data['country_code'] = user_data['country_code'].astype('category')
        user_data['country_code'] = user_data['country_code'].apply(lambda x: x.replace('GGB', 'GB'))

        user_data['address'] = user_data['address'].apply(lambda x: x.replace("\n", ", "))
        user_data['address'] = user_data['address'].apply(lambda x: x.replace("/", ""))
        user_data['address'] = user_data['address'].apply(lambda x: x.lower())
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

        data = read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages='all', output_format='dataframe')
        card_data = pd.DataFrame(data[0])
        card_data.dropna(inplace=True)
        card_data['card_provider'] = card_data['card_provider'].astype('category')

        card_data['card_number'] = card_data['card_number'].apply(lambda x: x.replace('?', '')) #  1. removes ??? from card numbers  first
        card_data = card_data[card_data['card_number'].str.isnumeric()] # 2. then drops non numeric - must be in this order
        card_data['card_number'] = card_data['card_number'].astype('int64')

        card_data['expiry_date'] = card_data['expiry_date'].apply(lambda x: '01/' + x)
        card_data['expiry_date'] = pd.to_datetime(card_data['expiry_date'], dayfirst=True, format='mixed')
        card_data['date_payment_confirmed'] = pd.to_datetime(card_data['date_payment_confirmed'], dayfirst=True, format='mixed')

        card_data.reset_index(inplace=True, drop=True) # should go at the end of all cleaning

        return card_data