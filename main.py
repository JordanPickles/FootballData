from DatabaseFile import DatabaseConnector

import pandas as pd
import numpy as np

from sqlalchemy import create_engine, inspect
import psycopg2


if __name__ == "__main__":
    connector = DatabaseConnector()
    db_creds = connector.read_db_creds()
    engine = connector.init_db_engine(db_creds)