from sqlalchemy import create_engine, inspect, text
import psycopg2
from psycopg2 import sql
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Relationship
import yaml


class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open("Credentials.YAML", "r") as f:
            db_creds = yaml.safe_load(f)
        return db_creds
    
    def init_db_engine(self, db_creds):
        # Create a temporary engine to connect to the default 'postgres' database
        temp_engine = create_engine(f"postgresql+psycopg2://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/postgres")
        
        # Check if the target database exists and create it if it doesn't
        with temp_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :dbname"), {'dbname': db_creds['LOCAL_DATABASE']})
            if result.scalar() is None:
                self.create_new_db(db_creds['LOCAL_USER'], db_creds['LOCAL_PASSWORD'], db_creds['LOCAL_HOST'], db_creds['LOCAL_PORT'], 'postgres', db_creds['LOCAL_DATABASE'])
                print(f"Database '{db_creds['LOCAL_DATABASE']}' created successfully!")
            else:
                print(f"Database '{db_creds['LOCAL_DATABASE']}' already exists.")

        # Create the engine for the target database
        engine = create_engine(f"{db_creds['LOCAL_DATABASE_TYPE']}+{db_creds['LOCAL_DB_API']}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        connection = engine.connect()
        print("Connected to the database successfully!")
        
        return engine, connection
    
    def create_replace_db_table(self, data_frame, table_name, engine):
        data_frame.to_sql(table_name, engine, if_exists='replace')

    def create_new_db(self, local_user, local_password, local_host, local_port, local_database, new_db_name):
        conn = psycopg2.connect(
            user =  local_user,
            password = local_password,
            host = local_host,
            port = local_port,
            database = local_database 
            )
        
        conn.autocommit = True #Set to autocommit mode to ensure the action is implemented straight away

        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
        conn.close

Base = declarative_base()

class ShotDataTable(Base):
    __tablename__ = 'dim_shots'
    
    shot_id = Column(Integer, primary_key=True)
    minute = Column(Integer)
    result = Column(String)
    X = Column(Float)
    Y = Column(Float)
    xG = Column(Float)
    player = Column(String)
    h_a = Column(String)
    player_id = Column(Integer)
    situation = Column(String)
    season = Column(Integer)
    shot_type = Column(String)
    match_id = Column(Integer, ForeignKey('dim_match_data.match_id'))
    last_action = Column(String)
    player_team = Column(String)
    player_assisted = Column(String)
    date = Column(Date)
    league = Column(String)

    match = Relationship("MatchDataTable", back_populates="shots")

class MatchDataTable(Base):
    __tablename__ = 'dim_match_data'
    
    match_id = Column(Integer, primary_key=True)
    h_team = Column(String)
    a_team = Column(String)
    h_goals = Column(Integer)
    a_goals = Column(Integer)
    date = Column(Date)
    league = Column(String)
    season = Column(Integer)

    shots = Relationship("ShotDataTable", back_populates="match")