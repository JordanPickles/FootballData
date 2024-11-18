import psycopg2
from psycopg2 import sql
from DatabaseFile import DatabaseConnector
from datetime import datetime

class DatabaseQueries:
    def __init__(self, db_creds):
        self.dbname = db_creds['LOCAL_DATABASE']
        self.user = db_creds['LOCAL_USER']
        self.password = db_creds['LOCAL_PASSWORD']
        self.host = db_creds['LOCAL_HOST']  
        self.port = db_creds['LOCAL_PORT']
        self.connection = None

    def table_connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def get_max_date(self):
        if not self.connection:
            return None

        try:
            cursor = self.connection.cursor()
            query = sql.SQL("SELECT MAX(datetime) FROM dim_match")
            cursor.execute(query)
            max_date = cursor.fetchone()[0]
            cursor.close()
            if max_date == None:
                return datetime(1900, 1, 1) 
            return max_date
        except Exception as e:
            print(f"Error executing query: {e}")
            return 0

    def close(self):
        if self.connection:
            self.connection.close()

if __name__ == "__main__":
    db_connector = DatabaseConnector()
    db_creds = db_connector.read_db_creds()

    db_queries = DatabaseQueries(dbname=db_creds['LOCAL_DATABASE'], user=db_creds['LOCAL_USER'], password=db_creds['LOCAL_PASSWORD'], host=db_creds['LOCAL_HOST'], port=db_creds['LOCAL_PORT'])
    db_queries.table_connect()
    max_date = db_queries.get_max_date()

    db_queries.close()
