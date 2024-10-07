# database.py
from sqlalchemy import create_engine, MetaData
from databases import Database

DATABASE_URL = "postgresql://postgress:12356Joe@localhost:5432/epita"

database = Database(DATABASE_URL)
metadata = MetaData()

# Sync version for creating tables
engine = create_engine(DATABASE_URL)