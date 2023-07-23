import pathlib

from sqlalchemy import Column, UniqueConstraint, String, Date
from sqlalchemy.dialects.mysql import BIGINT, FLOAT, INTEGER

import sys
sys.path.append(str(pathlib.Path(__file__).parents[1]))

from .db_config import Base, engine, db_obj


class WeatherData(Base):
    """Weather data table containing all the data from the files."""

    __tablename__ = 'weather_data'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    station = Column(String(255), nullable=False)
    max_temp = Column(FLOAT)
    min_temp = Column(FLOAT)
    precipitation_amt = Column(FLOAT)
    date = Column(Date)

    __table_args__ = (UniqueConstraint("station", "date", name="unique_rec"),)


class WeatherStats(Base):
    """Weather statistics table keeping the record of statistics."""

    __tablename__ = 'weather_stats'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    station = Column(String(255), nullable=False)
    year = Column(INTEGER)
    avg_max_temp = Column(FLOAT)
    avg_min_temp = Column(FLOAT)
    precipitation_amt = Column(FLOAT)

    __table_args__ = (UniqueConstraint("station", "year", name="unique_rec"),)


Base.metadata.create_all(bind=engine)
db_obj.commit()
