"""
Quick PostgreSQL connection test.
Run: python test_pg_connection.py
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

host     = os.getenv("PG_HOST")
port     = int(os.getenv("PG_PORT", 5432))
dbname   = os.getenv("PG_DATABASE")
user     = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")

print(f"Connecting to: {user}@{host}:{port}/{dbname}")

try:
    engine = create_engine(
        URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=dbname,
        ),
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
    )
    print("✅ Connection successful!\n")

    with engine.connect() as conn:
        tables = conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
                """
            )
        ).all()
    print(f"Tables in public schema ({len(tables)} found):")
    for t in tables:
        print(f"  - {t[0]}")

except SQLAlchemyError as e:
    print(f"❌ Connection failed: {e}")
