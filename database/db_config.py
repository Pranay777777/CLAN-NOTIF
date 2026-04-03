import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base

load_dotenv(".env")

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")
if not SQLALCHEMY_DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL in environment/.env")

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, future=True, pool_recycle=1800, pool_size=20, max_overflow=0
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

Base = declarative_base()
