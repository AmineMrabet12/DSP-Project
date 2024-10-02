import pandas as pd
from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from sqlalchemy.types import Boolean, DateTime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

csv_path = '../../data/churn.csv'
df = pd.read_csv(csv_path)

df = df.apply(pd.to_numeric, errors='ignore')

# Connect to your database
# DATABASE_URL = "postgresql://mohamedaminemrabet:amine@localhost:5432/epita"
# DATABASE_URL = "postgresql://wasedoo:postgres@localhost:5432/epita"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    else:
        return String


columns = []
for col_name, col_type in df.dtypes.items():
    sqlalchemy_type = map_dtype(col_type)

    if col_name == 'customerID':
        column = Column(col_name, sqlalchemy_type, primary_key=True, nullable=True)

    elif col_name == 'TotalCharges':
        column = Column(col_name, Float, primary_key=True, nullable=True)

    elif col_name == 'Churn':
        pass

    else:
        column = Column(col_name, sqlalchemy_type)
    
    columns.append(column)

prediction_column = Column('prediction', Float)
columns.append(prediction_column)


predictions = Table(
    "past_predictions",
    metadata,
    *columns
)

# Create the table in the database
metadata.create_all(engine)

print("Table created successfully.")
