from DatabaseFile import DatabaseConnector
from DataScraping import ShotDataScraper

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect
import psycopg2


if __name__ == "__main__":
    connector = DatabaseConnector()
    shot_data_scraper = ShotDataScraper()
    db_creds = connector.read_db_creds()
    engine, connection = connector.init_db_engine(db_creds)

    df_shot_data = shot_data_scraper.collect_shot_data()
    df_clean_shot_data = shot_data_scraper.clean_shot_data(df_shot_data)

    connector.create_replace_db_table(df_shot_data, 'dim_shots',engine)
