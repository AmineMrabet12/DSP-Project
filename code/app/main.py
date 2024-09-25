from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from database import database, engine, metadata
from models import predictions
from joblib import load
from typing import List, Union

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

metadata.create_all(engine)


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

model = load('../../models/XGBoost_classifier.joblib')

@app.on_event("startup")
async def startup():
    await database.connect()   

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.post("/predict")
async def predict(input_data_list: Union[ModelInput, List[ModelInput]]):  # Accept a list of input data

    # print(input_data_list)
    if isinstance(input_data_list, list):  
        input_data_dicts = [item.dict() for item in input_data_list]
    else:
        input_data_dicts = [input_data_list.dict()]

    # input_data_dicts = [input_data.json() for input_data in input_data_list]#[0]
    # print(input_data_dicts)
    # input_data_dicts = [input_data.dict() for input_data in input_data_list]

    # Convert input data to DataFrame
    input_df = pd.DataFrame(input_data_dicts)
    # print(input_df)

    # Load preprocessing tools and column configurations
    ordinal = load('../../models/Ordinal_Encoder.joblib')
    scaler = load('../../models/Standard_Scaler.joblib')
    categorical_columns = load('../../models/categorical_columns.joblib')
    columns = load('../../models/columns.joblib')

    # Ensure the dataframe columns are in the correct order
    input_df = input_df[columns]

    # Apply transformations
    input_df[categorical_columns] = ordinal.transform(input_df[categorical_columns])
    input_df = scaler.transform(input_df)

    # Make predictions for all rows at once
    predictions_values = model.predict(input_df).tolist()
    # print(predictions_values)

    # Prepare the results for database insertion and return
    for idx, prediction_value in enumerate(predictions_values):
        input_data_dicts[idx]["prediction"] = int(prediction_value)
        
        query = predictions.insert().values(
            **input_data_dicts[idx]
        )
        await database.execute(query)

    return {"predictions": predictions_values}


@app.get("/past_predictions/")
async def get_predictions():
        query = predictions.select()
        results = await database.fetch_all(query)

        parsed_results = [
                dict(result)
                for result in results
        ]

        return parsed_results
