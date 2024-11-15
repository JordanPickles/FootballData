from DatabaseFile import DatabaseConnector, ShotDataTable, MatchDataTable
from DataScraping import UnderstatDataScraper

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect, text
import psycopg2
from psycopg2 import sql
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import yaml
import time
from datetime import datetime



if __name__ == "__main__":
    connector = DatabaseConnector()
    understat_data_scraper = UnderstatDataScraper()
    shot_data = ShotDataTable()
    db_creds = connector.read_db_creds()
    engine, connection = connector.init_db_engine(db_creds)
    
    Base = declarative_base()
    # Create all tables in the database if they do not exist
    Base.metadata.create_all(engine)

    league_list = ['EPL', 'La_Liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
    season = '2024'
 
    df_match_data, match_id_list = understat_data_scraper.match_id_retrieval(league_list, season)

    connector.create_replace_db_table(df_match_data, 'dim_match_data', engine)

    df_shot_data = understat_data_scraper.match_shots(match_id_list)

    df_clean_shot_data = understat_data_scraper.clean_shot_data(df_shot_data)
    connector.create_replace_db_table(df_clean_shot_data, 'dim_shots', engine)
