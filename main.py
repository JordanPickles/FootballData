import logging
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from DatabaseFile import DatabaseConnector
from DataScraping import UnderstatDataScraper
from DatabaseQueries import DatabaseQueries
from Models import Match, Shot, Base

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize connectors and scrapers
        connector = DatabaseConnector()
        understat_data_scraper = UnderstatDataScraper()
        
        # Read database credentials
        db_creds = connector.read_db_creds()
        
        # Establish database connections
        psycopg2_connection = connector.psycopg2_connect(db_creds)
        engine, connection = connector.init_db_engine(db_creds, psycopg2_connection)
        
        # Initialize database queries
        db_queries = DatabaseQueries(psycopg2_connection)
        
        # Create all tables in the database if they do not exist
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created")
        
        # Get the maximum date from the database
        max_date = db_queries.get_max_date()
        logger.info(f"Max date: {max_date}")
        
        # Close the psycopg2 connection as no longer needed
        psycopg2_connection.close()
        
        # Define league list and season to be used in subsequent functions
        league_list = ['EPL', 'La_Liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
        season = '2024'
        
        # Retrieve match data
        df_match_data, match_id_list = understat_data_scraper.match_id_retrieval(league_list, season, max_date)
        
        # Append match data to the database if not empty
        if not df_match_data.empty:
            connector.append_db_table(df_match_data, 'dim_match', engine)
            logger.info("Match data appended to database")
            
            # Retrieve and clean shot data
            df_shot_data = understat_data_scraper.match_shots(match_id_list, season)
            df_clean_shot_data = understat_data_scraper.clean_shot_data(df_shot_data)
            
            # Append shot data to the database if not empty
            if not df_clean_shot_data.empty:
                connector.append_db_table(df_clean_shot_data, 'dim_shot', engine)
                logger.info("Shot data appended to database")
            else:
                logger.info("No shot data to insert into the database.")

            df_league_table = understat_data_scraper.league_table(df_match_data, league_list)
            
            # Append league table data to the database if not empty
            if not df_league_table.empty:
                connector.append_db_table(df_league_table, 'dim_league_table', engine)
                logger.info("League table data appended to database")
            else:
                logger.info("No league table data to insert into the database.")

        else:
            logger.info("No match data to insert into the database.")
        
        # Close the SQLAlchemy connection
        connection.close()
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()