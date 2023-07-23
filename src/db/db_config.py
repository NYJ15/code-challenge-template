"""
Config File
"""
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

SQLALCHEMY_DATABASE_URI = "sqlite:///./local.db"

SQLALCHEMY_TRACK_MODIFICATIONS = False
DATABASE_CONNECT_OPTIONS = {}
CSRF_ENABLED = True
CSRF_SESSION_KEY = ""
SQLALCHEMY_POOL_RECYCLE = 28750

engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_size=20,
                       max_overflow=0, pool_pre_ping=True)
db_obj = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()
Base.query = db_obj.query_property()
