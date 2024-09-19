import pandas as pd
from sqlalchemy import Table, Column, Integer, String, Float, MetaData
from sqlalchemy.types import Boolean, DateTime
from sqlalchemy import create_engine

# Load CSV and infer column data types
csv_path = '../../data/churn.csv'  # Replace with the path to your CSV file
df = pd.read_csv(csv_path)

# Convert columns with errors in numeric conversion to NaN
df = df.apply(pd.to_numeric, errors='ignore')

# Connect to your database
DATABASE_URL = "postgresql://mohamedaminemrabet:amine@localhost:5432/epita"  # Update this
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Function to map pandas dtypes to SQLAlchemy types
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
        return String  # Fallback for object or string types

# Create columns dynamically based on the CSV structure
columns = []
for col_name, col_type in df.dtypes.items():
    sqlalchemy_type = map_dtype(col_type)

    # Check if this is the column you want to set as primary key
    if col_name == 'customerID':  # Set your column name here
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

# Define the table dynamically using the existing primary key column
predictions = Table(
    "past_predictions",
    metadata,
    *columns  # Unpacking the list of columns
)

# Create the table in the database
metadata.create_all(engine)

print("Table created successfully.")
