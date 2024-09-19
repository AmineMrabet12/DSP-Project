# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from database import database, engine, metadata
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from models import predictions
from joblib import load
import json
import numpy as np

# Initialize FastAPI and bind the metadata to create tables if they don't exist
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],  # Update this to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

metadata.create_all(engine)

# Model input schema
class ModelInput(BaseModel):
    customerID: str
    gender: str
    SeniorCitizen: int
    Partner: str
    Dependents: str
    tenure: int
    PhoneService: str
    MultipleLines: str
    InternetService: str
    OnlineSecurity: str
    OnlineBackup: str
    DeviceProtection: str
    TechSupport: str
    StreamingTV: str
    StreamingMovies: str
    Contract: str
    PaperlessBilling: str
    PaymentMethod: str
    MonthlyCharges: float
    TotalCharges: float

# Load your saved model
model = load('../../models/XGBoost_classifier.joblib')

# Connect to the database on startup and disconnect on shutdown
@app.on_event("startup")
async def startup():
    await database.connect()   

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# Endpoint 1: Predict and save result in the database
@app.post("/predict")
async def predict(input_data_: ModelInput):

        input_data_dict = input_data_.dict()

        # def convert_value(value):
        #     if isinstance(value, np.int64):
        #         return int(value)
        #     elif isinstance(value, np.float64):
        #         return float(value)
        #     elif isinstance(value, (np.ndarray, pd.Series)):
        #         return value.tolist()  # If it's a numpy array or pandas series, convert to list
        #     return value

        # input_data_dict = {key: convert_value(value) for key, value in input_data_dict.items()}


        input_df = pd.DataFrame([input_data_dict])

        ordinal = load('../../models/Ordinal_Encoder.joblib')
        scaler = load('../../models/Standard_Scaler.joblib')
        categorical_columns = load('../../models/categorical_columns.joblib')
        columns = load('../../models/columns.joblib')

        # print("columns", columns)
        # print("categorical_columns", categorical_columns)

        input_df = input_df[columns]

        input_df[categorical_columns] = ordinal.transform(input_df[categorical_columns])
        input_df = scaler.transform(input_df)

        prediction_value = model.predict(input_df)[0].item()
        print(prediction_value)

        # combined_data = input_data_.dict()
        combined_data = input_data_dict

        combined_data["prediction"] = int(prediction_value)

        # print(pd.DataFrame([combined_data]).dtypes)

        # Convert the combined data to a JSON string
        # combined_data = {
        #     key: (value.item() if isinstance(value, (str)) else value)
        #     for key, value in combined_data.items()
        # }

        # combined_data = pd.DataFrame([combined_data])
        # combined_data = pd.to_numeric(combined_data, errors='coerce')
        # combined_json = {
        #     key: (value.item() if isinstance(value, (int, float)) else value)
        #     for key, value in combined_data.items()
        # }
        # value_types = {key: type(value) for key, value in combined_data.items()}

        # print(value_types)
        # print(combined_data)

        # Insert the combined data into the database
        query = predictions.insert().values(
                # json.dumps(combined_json)
                **combined_data
        )

        # print(json.dumps(combined_data))
        await database.execute(query)

        return {"input": input_data_dict, "prediction": prediction_value}

# Endpoint 2: Get all saved predictions
@app.get("/past_predictions/")
async def get_predictions():
    # Select all rows from the predictions table
        query = predictions.select()
        results = await database.fetch_all(query)

        # Convert results to a list of dictionaries, where each dictionary represents a row
        parsed_results = [
                dict(result)  # Convert each row to a dictionary
                for result in results
        ]

        return parsed_results
