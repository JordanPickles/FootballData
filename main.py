from DatabaseFile import DatabaseConnector
from DataScraping import UnderstatDataScraper
from DatabaseQueries import DatabaseQueries
from Models import Match, Shot, Base

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect, text
import psycopg2
from psycopg2 import sql
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, DeclarativeBase
import yaml
import time
from datetime import datetime



if __name__ == "__main__":
    connector = DatabaseConnector()
    understat_data_scraper = UnderstatDataScraper()
    db_creds = connector.read_db_creds()
    engine, connection = connector.init_db_engine(db_creds)
    db_queries = DatabaseQueries(db_creds)
    
    Base.metadata.create_all(bind=engine)

    db_queries.table_connect()
    max_date = db_queries.get_max_date()

    league_list = ['EPL', 'La_Liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
    season = '2024'

    df_match_data, match_id_list = understat_data_scraper.match_id_retrieval(league_list, season, max_date)

    if not df_match_data.empty:
        connector.create_replace_db_table(df_match_data, 'dim_match', engine)
        df_shot_data = understat_data_scraper.match_shots(match_id_list, season)
        df_clean_shot_data = understat_data_scraper.clean_shot_data(df_shot_data)
        connector.create_replace_db_table(df_clean_shot_data, 'dim_shot', engine)
    else:
        print("No new matches to add to the database.")
