import pandas as pd
import numpy as np
import yaml
from sqlalchemy import create_engine, inspect
import psycopg2
import os


class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open("Credentials.YAML", "r") as f:
            db_creds = yaml.safe_load(f)
        return db_creds
    
    def init_db_engine(self, db_creds):
        # Create a database engine
        engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        # Connect to the database
        connection = engine.connect()
        # Print connection status
        print("Connected to the database successfully!")
        # Take in the db_creds output and initialise and return an sql_alchemy database engine
        return connection
    
    def upload_to_db(self, data_frame, table_name, db_creds):
        local_engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        local_engine.connect()
        data_frame.to_sql(table_name, local_engine, if_exists='replace')