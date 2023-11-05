from database_utils import DatabaseConnector as dc
from botocore.exceptions import NoCredentialsError
from sqlalchemy import text
from tabula import read_pdf
import pandas as pd
import requests as rq
import boto3

class DataExtractor:
    def __init__(self) -> None:
        self.engine = dc()
        self.conn_instance = self.engine.init_db_engine()        
    def read_rds_table(self, table_name):
        """Takes 2 args: Instance of the 
        DatabaseConnector in database_utils.py
        and name of a table in RDS database. 
        Returns a Pandas Dataframe """

        query = f"SELECT * FROM {table_name}"
           
        with self.conn_instance.connect() as connection:
            data = connection.execute(text(query))
            return pd.DataFrame(data)
        
    def retrieve_pdf_data(self, link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        data = read_pdf(link, pages='all', output_format='dataframe')
        pdf_df = pd.DataFrame(data[0])
        return pdf_df
    
    def list_number_of_stores(self, num_stores_endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', api_key={"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        response = rq.get(num_stores_endpoint, headers=api_key)
        stores = response.json()
        return stores['number_stores']
    
    def retrieve_stores_data(self, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'):
        
        num_stores = self.list_number_of_stores()
        stores_list = []
        for n in range(0, num_stores):
            response = rq.get(f'{endpoint}{n}', headers={"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"})
            store_details = response.json()
            stores_list.append(store_details)
        df = pd.DataFrame.from_dict(stores_list)
        return df
    
    def extract_from_s3(self, s3_address='s3://data-handling-public/products.csv'):
        
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


        
    
        
        
        


    


