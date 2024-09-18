# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from database import database, engine, metadata
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from models import predictions
from joblib import load
import json

# Initialize FastAPI and bind the metadata to create tables if they don't exist
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update this to specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

metadata.create_all(engine)

# Model input schema
class ModelInput(BaseModel):
    gender: object
    SeniorCitizen: int
    Partner: object
    Dependents: object
    tenure: int
    PhoneService: object
    MultipleLines: object
    InternetService: object
    OnlineSecurity: object
    OnlineBackup: object
    DeviceProtection: object
    TechSupport: object
    StreamingTV: object
    StreamingMovies: object
    Contract: object
    PaperlessBilling: object
    PaymentMethod: object
    MonthlyCharges: float
    TotalCharges: object
    Churn: object

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
@app.post("/predict/")
async def predict(input_data: ModelInput):
    input_df = pd.DataFrame([input_data.dict()])

    ordinal = load('../../models/Ordinal_Encoder.joblib')
    scaler = load('../../models/Standard_Scaler.joblib')
    categorical_columns = load('../../models/categorical_columns.joblib')

    input_df = input_df[categorical_columns]

    input_df['Churn'] = input_df['Churn'].replace({'Yes': 1, 'No': 0})
#     input_df = input_df.dropna()

    input_df[categorical_columns] = ordinal.transform(input_df[categorical_columns])
    input_df = scaler.transform(input_df)



    prediction_value = model.predict(input_df)[0]

    # Save prediction to database
    query = predictions.insert().values(
        input_data=json.dumps(input_data.dict()),  # Save input data as JSON string
        prediction=prediction_value
    )
    await database.execute(query)

    return {"input": input_data.dict(), "prediction": prediction_value}

# Endpoint 2: Get all saved predictions
@app.get("/past_predictions/")
async def get_predictions():
    query = predictions.select()
    results = await database.fetch_all(query)

    # Parse the input_data field (saved as JSON string) back to a dictionary
    parsed_results = [
        {
            "customerID": result["customerID"],  # Assuming 'customerID' is the primary key
            "input_data": json.loads(result["input_data"]),
            "prediction": result["prediction"]
        }
        for result in results
    ]

    return parsed_results
