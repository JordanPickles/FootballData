import psycopg2
from psycopg2 import sql
from DatabaseFile import DatabaseConnector
from datetime import datetime
import pandas as pd

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
        
    def league_table_view(self):

        try:
            # Read the SQL script from the file
            with open('League_Table_Create.sql', 'r') as file:
                league_table_query = file.read()
            
            cursor = self.connection.cursor()

            result = cursor.execute(league_table_query)
            self.connection.commit()  
            cursor.close() 
            
        except Exception as e:
            print(f"Error executing query: {e}")

    def dashboard_data_export(self):
        try:
            # Read the SQL script from the file
            with open('Shots_Trellis_Dashboard_Data_Query.sql', 'r') as file:
                dashboard_query = file.read()
            
            cursor = self.connection.cursor()

            # Execute the query
            cursor.execute(dashboard_query)
            
            # Fetch all rows and column names
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            
            # Create the DataFrame
            df_shots_dashboard = pd.DataFrame(rows, columns=column_names)
            
            # Save to CSV
            output_path = '/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/FootballData/Shots_Trellis_Dashboard_Data.csv'
            df_shots_dashboard.to_csv(output_path, index=False)
            
            print(f"Data successfully saved to {output_path}")
            cursor.close()
            
        except Exception as e:
            print(f"Error executing query: {e}")
        