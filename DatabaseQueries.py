import psycopg2
from psycopg2 import sql
from DatabaseFile import DatabaseConnector
from datetime import datetime
import pandas as pd

class DatabaseQueries:
    def __init__(self, psycopg2_connection):
        """Initializes the DatabaseQueries class with a psycopg2 connection."""
        self.connection = psycopg2_connection
    
    def get_max_date(self):
        """
        Retrieves the maximum date from the dim_match table.
        Returns:
            datetime: The maximum date found in the dim_match table.
            If no date is found, returns datetime(1900, 1, 1).
            If an error occurs, returns 0."""
        
        if not self.connection:
            return None
        try:
            cursor = self.connection.cursor()
            query = sql.SQL("SELECT MAX(datetime) FROM dim_match") # Gets the max date from the database table
            cursor.execute(query)
            max_date = cursor.fetchone()[0]
            cursor.close()
            if max_date == None or max_date == 0:
                return datetime(1900, 1, 1) # Returns a date should no date be found: e.g. the first time the code is run so that the rest of the code can stil run returning all matches
            return max_date
        except Exception as e:
            print(f"Error executing query: {e}")
            return 0
        
    def league_table_view(self):
        """
        Executes the SQL script to create or update the league table view.
        Reads the SQL script from 'League_Table_Create.sql' file.
        If an error occurs, prints the error message."""

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
        """
        Executes the SQL script to fetch data for the shots trellis dashboard.
        Reads the SQL script from 'Shots_Trellis_Dashboard_Data_Query_V2.sql' file.
        Fetches the data, converts it to a pandas DataFrame, and saves it as a CSV file.
        If an error occurs, prints the error message."""
        try:
            with open('Shots_Trellis_Dashboard_Data_Query_V2.sql', 'r') as file:
                dashboard_query = file.read() # Reads the SQL script from the file
    
            cursor = self.connection.cursor()
            cursor.execute(dashboard_query) #Executes the query
            rows = cursor.fetchall()  # Fetches all rows and column names
            column_names = [desc[0] for desc in cursor.description] # Returns all the column names in the table
            df_shots_dashboard = pd.DataFrame(rows, columns=column_names)
            output_path = '/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/FootballData/Shots_Trellis_Dashboard_Data.csv'
            df_shots_dashboard.to_csv(output_path, index=False)
            
            print(f"Data successfully saved to {output_path}")
            cursor.close()
            
        except Exception as e:
            print(f"Error executing query: {e}")
        