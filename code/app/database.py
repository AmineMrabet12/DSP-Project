from sqlalchemy import create_engine, MetaData
from databases import Database

DATABASE_URL = "postgresql://mohamedaminemrabet:amine@localhost:5432/epita"
# DATABASE_URL = "postgresql://wasedoo:postgres@localhost:5432/epita"

database = Database(DATABASE_URL)
metadata = MetaData()

# Sync version for creating tables
engine = create_engine(DATABASE_URL)
