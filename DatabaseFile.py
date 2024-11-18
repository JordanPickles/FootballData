from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import psycopg2
from psycopg2 import sql
import os

import yaml

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open("Credentials.YAML", "r") as f:
            db_creds = yaml.safe_load(f)
        return db_creds
    
    def init_db_engine(self, db_creds):
        # Create a temporary engine to connect to the default 'postgres' database
        temp_engine = create_engine(f"postgresql+psycopg2://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/postgres")
        
        # Check if the target database exists and create it if it doesn't
        with temp_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :dbname"), {'dbname': db_creds['LOCAL_DATABASE']})
            if result.scalar() is None:
                self.create_new_db(db_creds['LOCAL_USER'], db_creds['LOCAL_PASSWORD'], db_creds['LOCAL_HOST'], db_creds['LOCAL_PORT'], 'postgres', db_creds['LOCAL_DATABASE'])
                print(f"Database '{db_creds['LOCAL_DATABASE']}' created successfully!")
            else:
                print(f"Database '{db_creds['LOCAL_DATABASE']}' already exists.")

        # Create the engine for the target database
        engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        connection = engine.connect()
        print("Connected to the database successfully!")
        
        return engine, connection
    
    def create_replace_db_table(self, data_frame, table_name, engine):
        batch_size=1000        
        try:
            data_frame.to_sql(table_name, engine, if_exists='append', index=False, chunksize=batch_size)
            print(f"Data appended to table {table_name} successfully!")
        except Exception as e:
            print(f"Error appending data to table {table_name}: {e}")
    
    def create_new_db(self, local_user, local_password, local_host, local_port, local_database, new_db_name):
        conn = psycopg2.connect(
            user =  local_user,
            password = local_password,
            host = local_host,
            port = local_port,
            database = local_database 
            )
        
        conn.autocommit = True #Set to autocommit mode to ensure the action is implemented straight away

        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
        conn.close

