from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column, Relationship, DeclarativeBase
from sqlalchemy.dialects.postgresql import TEXT, VARCHAR, INTEGER, FLOAT, DATE, TIMESTAMP
from typing import List
from typing import Optional

class Base(DeclarativeBase):
    type_annotation_map={
        int: INTEGER,
        str: VARCHAR,
        float: FLOAT,
        Date: DATE,
        DateTime: TIMESTAMP
    }

class Match(Base):
    __tablename__ = 'dim_match'
    
    match_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    datetime: Mapped[DateTime]
    league: Mapped[str] = mapped_column(String(10))
    home_team_id: Mapped[int]
    home_team_name: Mapped[str] = mapped_column(String(30))
    home_team_name_short: Mapped[str] = mapped_column(String(3))
    away_team_id: Mapped[int]
    away_team_name: Mapped[str] = mapped_column(String(30))
    away_team_name_short: Mapped[str] = mapped_column(String(3))
    home_team_goals: Mapped[int]
    away_team_goals: Mapped[int]
    home_team_xg: Mapped[float]
    away_team_xg: Mapped[float]
    win_forecast: Mapped[float]
    draw_forecast: Mapped[float]
    loss_forecast: Mapped[float]
    season: Mapped[int]
    shots: Mapped[List["Shot"]] = Relationship(back_populates="match")

class Shot(Base):
    __tablename__ = 'dim_shot'

    shot_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    minute: Mapped[int]
    result: Mapped[str] = mapped_column(String(30))
    x: Mapped[float]
    y: Mapped[float]
    xg: Mapped[float]
    player: Mapped[str] = mapped_column(String(50))
    h_a: Mapped[str] = mapped_column(String(1))
    player_id: Mapped[int]
    situation: Mapped[str] = mapped_column(String(20))
    season: Mapped[int]
    shot_type: Mapped[str] = mapped_column(String(20))
    last_action: Mapped[str] = mapped_column(String(20))
    player_team: Mapped[str] = mapped_column(String(30))
    player_assisted: Mapped[str] = mapped_column(String(50))
    date: Mapped[Date]
    league: Mapped[str] = mapped_column(String(10))
    team_against: Mapped[str] = mapped_column(String(30))
    match_id: Mapped[int] = mapped_column(ForeignKey('dim_match.match_id'))
    season: Mapped[int]
    match: Mapped["Match"] = Relationship(back_populates="shots")

