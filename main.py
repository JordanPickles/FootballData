from DatabaseFile import DatabaseConnector, ShotDataTable
from DataScraping import UnderstatDataScraper

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect
import psycopg2


if __name__ == "__main__":
    connector = DatabaseConnector()
    understat_data_scraper = UnderstatDataScraper()
    shot_data = ShotDataTable()
    db_creds = connector.read_db_creds()
    engine, connection = connector.init_db_engine(db_creds)
    
    league_list = ['EPL', 'La_Liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
    season = '2024'

    df_match_data, match_id_list = understat_data_scraper.match_id_retrieval(league_list, season)
    df_shot_data = understat_data_scraper.collect_shot_data()
    df_clean_shot_data = understat_data_scraper.clean_shot_data(df_shot_data)
    df_match_data.to_csv('/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/FootballData/match_data_2024.csv', index=False)
    connector.create_replace_db_table(df_clean_shot_data, 'dim_shots', engine)
