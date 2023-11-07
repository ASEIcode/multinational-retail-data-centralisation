from database_utils import DatabaseConnector as dc
from botocore.exceptions import NoCredentialsError
from sqlalchemy import text
from tabula import read_pdf
import pandas as pd
import requests as rq
import boto3

class DataExtractor:
    """
    Contains methods for extracting data from various formats and returns each as a pandas dataframe.
    """

    def __init__(self) -> None:
        """
        Create an instance of the DatabaseConnector class and initialise the engine.
        """

        self.engine = dc()
        self.conn_instance = self.engine.init_db_engine()  


    def read_rds_table(self, table_name):
        """
        Reads an AWS RDS table and Returns a Dataframe.

        Keyword argument:        
            - table_name -- Name of a table in RDS database. 

        Return a Pandas Dataframe 
        """

        query = f"SELECT * FROM {table_name}"
           
        with self.conn_instance.connect() as connection:
            data = connection.execute(text(query))
            return pd.DataFrame(data)
        

    def retrieve_pdf_data(self, link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        """
        Takes a link to a pdf as arg. Returns a Pandas Dataframe. 
        """
        data = read_pdf(link, pages='all', output_format='dataframe')
        pdf_df = pd.DataFrame(data[0])
        return pdf_df
    

    def list_number_of_stores(self, num_stores_endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_key={"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        """
        Requests the number of stores using an api.

        Keyword arguments:
            - num_stores_endpoint -- The Endpoint link used to retrive the number of stores via the api (default https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores)
            - api_key -- The API key used in the header for access permission (default {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})

        Return -> The number of stores (int) from a json as stores['number_stores'].
        """
        response = rq.get(num_stores_endpoint, headers=api_key)
        stores = response.json()
        return stores['number_stores']
    
    
    def retrieve_stores_data(self, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'):
        """
        Retrieves the number of stores and compiles a dataframe with details about each store.

        Uses list_number_of_stores() method to retrieve the number of stores. Creates a list of dictionaries 
        of the store details and converts this list into a pandas dataframe.

        Argument:
        endpoint -- link to the endpoint holding the stores data.

        Return -> Dataframe as df
        """

        num_stores = self.list_number_of_stores()
        stores_list = []
        for n in range(0, num_stores):
            response = rq.get(f'{endpoint}{n}', headers={"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
            store_details = response.json()
            stores_list.append(store_details)
        df = pd.DataFrame.from_dict(stores_list)
        return df
    

    def extract_from_s3(self, s3_address='s3://data-handling-public/products.csv'):
        """
        Extracts a csv file from an AWS s3 bucket.

        Keyword argument:
        s3_address -- the s3 address of the bucket (default s3://data-handling-public/products.csv)

        try:
        Download the csv file from the bucket. If it is successful, prints a message.

        Exceptions:
            - NoCredentialsError -- If your AWS credentials are not present or incorrect. Use AWS config to configure these.
            - Exception -- handles and prints any other error
        
        Return -> products_df (pandas dataframe)
        """
        
        bucket_name = s3_address.split('/')[2]
        object_key = s3_address.split('/')[3]
        # Initialize an S3 client
        s3 = boto3.client('s3')

        try:
            # Download the file from S3
            s3.download_file(bucket_name, object_key, 'products.csv')
            print("File 'products.csv' downloaded successfully.")
        except NoCredentialsError:
            print("AWS credentials not found. Make sure your credentials are configured.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

        products_df = pd.read_csv('products.csv')
        return products_df


    def extract_json_from_link(self, endpoint='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        """
        Extracts a json file from a public web link and returns it as a pandas dataframe.

        Keyword argument:
        endpoint -- The endpoint link where the json is located (default https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json)

        Return -> events_data as a dataframe.
        """
        response = rq.get(f'{endpoint}')
        events_data = response.json()
        return pd.DataFrame.from_dict(events_data)