from database_utils import DatabaseConnector as dc
from sqlalchemy import text
import pandas as pd

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
        
        


    


