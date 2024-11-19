import psycopg2
from psycopg2 import sql
from DatabaseFile import DatabaseConnector
from datetime import datetime

class DatabaseQueries:
    def __init__(self, psycopg2_connection):
        self.connection = psycopg2_connection
    
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

