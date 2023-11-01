from database_utils import DatabaseConnector as dc
from sqlalchemy import text
from tabula import read_pdf
import pandas as pd
import requests as rq

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
    
        
        
        


    


