from data_extraction import DataExtractor as de
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

        return user_data