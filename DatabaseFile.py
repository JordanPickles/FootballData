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
        """
        Reads database credentials from a YAML file.
        This method opens the "Credentials.YAML" file, reads its contents,
        and parses it as a YAML file to extract the database credentials.
        Returns:
            dict: A dictionary containing the database credentials.
        """
        with open("Credentials.YAML", "r") as f:
            db_creds = yaml.safe_load(f)
        return db_creds
    
    def init_db_engine(self, db_creds, psycopg2_connection):
        """
        Initializes the database engine and connects to the specified database.
        Parameters:
            db_creds (dict): A dictionary containing database credentials and configuration.
                Expected keys are:
                    - 'LOCAL_USER': Username for the database.
                    - 'LOCAL_PASSWORD': Password for the database.
                    - 'LOCAL_HOST': Host address of the database.
                    - 'LOCAL_PORT': Port number of the database.
                    - 'LOCAL_DATABASE': Name of the target database.
                    - 'LOCAL_DATABASE_TYPE': Type of the database (e.g., 'postgresql').
                    - 'LOCAL_DB_API': Database API to use (e.g., 'psycopg2').
            psycopg2_connection: A psycopg2 connection object used to create a new database if it doesn't exist.
        Returns:
            tuple: A tuple containing the SQLAlchemy engine and the connection object for the target database.
        Raises:
            Exception: If there is an error initializing the database engine.
        """
        try:
            # Create a temporary engine to connect to the default 'postgres' database
            temp_engine = create_engine(f"postgresql+psycopg2://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/postgres")
            
            # Check if the target database exists and create it if it doesn't
            with temp_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :dbname"), {'dbname': db_creds['LOCAL_DATABASE']})
                if result.scalar() is None:
                    self.create_new_db(psycopg2_connection, db_creds)
                    print(f"Database '{db_creds['LOCAL_DATABASE']}' created successfully!")
                else:
                    print(f"Database '{db_creds['LOCAL_DATABASE']}' already exists.")

            # Create the engine for the target database
            engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
            connection = engine.connect()
            print("Connected to the database successfully!")
            
            return engine, connection
        
        except Exception as e:
            print(f"Error initializing database engine: {e}")
    
    def psycopg2_connect(self, db_creds):
        """
        Establishes a connection to a PostgreSQL database using psycopg2.
        Parameters:
            db_creds (dict): A dictionary containing database credentials with the following keys:
            - 'LOCAL_DATABASE': Name of the database.
            - 'LOCAL_USER': Username for the database.
            - 'LOCAL_PASSWORD': Password for the database.
            - 'LOCAL_HOST': Host address of the database.
            - 'LOCAL_PORT': Port number of the database.
        Returns:
            connection: A psycopg2 connection object if the connection is successful.
        Raises:
            Exception: If there is an error connecting to the database, an exception is raised and the error message is printed.
        """

        # Connects to the database
        try:
            connection = psycopg2.connect(
            dbname = db_creds['LOCAL_DATABASE'],
            user = db_creds['LOCAL_USER'],
            password = db_creds['LOCAL_PASSWORD'],
            host = db_creds['LOCAL_HOST'],
            port = db_creds['LOCAL_PORT']
            )
            print("Connected to the database successfully!")
            return connection
        except Exception as e:
            print(f"Error connecting to database: {e}")
    
    def create_new_db(self, psycopg2_connection, new_db_name):
        """
        Creates a new PostgreSQL database.
        Parameters:
            psycopg2_connection (psycopg2.extensions.connection): The connection object to the PostgreSQL server.
            new_db_name (str): The name of the new database to be created.
        Returns:
            None
        Raises:
            Exception: If there is an error creating the database.
        """
        try:
            psycopg2_connection.autocommit = True  # Set to autocommit mode to ensure the action is implemented straight away

            with psycopg2_connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
            psycopg2_connection.close()
            print(f"Database '{new_db_name}' created successfully!")
        
        except Exception as e:
            print(f"Error creating database '{new_db_name}': {e}")

    def append_db_table(self, data_frame, table_name, engine):
        """
        Appends data from a DataFrame to a specified table in a database.
        Parameters:
            data_frame (pandas.DataFrame): The DataFrame containing the data to be appended.
            table_name (str): The name of the table to which the data will be appended.
            engine (sqlalchemy.engine.Engine): The SQLAlchemy engine connected to the database.
        Returns:
            None
        Raises:
            Exception: If there is an error appending the data to the table, an exception is caught and an error message is printed.
        """
        batch_size=1000        
        try:
            data_frame.to_sql(table_name, engine, if_exists='append', index=False, chunksize=batch_size)
            print(f"Data appended to table {table_name} successfully!")
        except Exception as e:
            print(f"Error appending data to table {table_name}: {e}")
    

    def close(self, connection):
            """
            Closes the given database connection.
            Parameters:
                connection (object): The database connection object to be closed.
            """
            connection.close()

