from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect

import yaml

class DatabaseConnector:
    def __init__(self) -> None:
        pass
    def read_db_creds(self):
        """Reads and Returns the AWS RDS Credentials
        from the YAML file (not included in Git Repo)
        """
        with open("db_creds.yaml", "r") as f:
            creds = yaml.safe_load(f)
            return creds

    def init_db_engine(self):
        """Use to create and return the SqlAlchemy engine 
        to access the AWS RDS database. Calls read_db_creds() 
        to construct to database credentials URL"""
        creds = self.read_db_creds()
        HOST = creds["RDS_HOST"]
        PASSWORD = creds["RDS_PASSWORD"]
        USER = creds["RDS_USER"]
        DATABASE = creds["RDS_DATABASE"]
        PORT = creds["RDS_PORT"]
        URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        self.engine = create_engine(URL)
        return self.engine
    
    def list_db_tables(self):
        """Returns the names of the tables in the AWS RDS Database"""
        inspector = inspect(self.engine)
        print(inspector)
        print(inspector.get_table_names())
    
    def make_query(self, query):
        """Takes a single string argument (Your SQL Query).
        Returns the result of the as 'data'"""
        with self.engine.connect() as connection:
            data = connection.execute(text(query))
            return data


    
       




