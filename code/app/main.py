from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from database import database, engine, metadata
from models import predictions
from joblib import load
from typing import List, Union
from fastapi import Query
from sqlalchemy.sql import and_
from datetime import date, datetime


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8000/"],
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

model = load('../../models/grid_search.joblib')

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.post("/predict")
async def predict(input_data_list: Union[ModelInput, List[ModelInput]],
                  request: Request):  # Accept a list of input data

    source_prediction = "Scheduled Predictions"  # Default for Airflow
    if request.headers.get("X-Source") == "streamlit":
        source_prediction = "Web Application"

    # print(input_data_list)
    if isinstance(input_data_list, list):
        input_data_dicts = [item.dict() for item in input_data_list]
        input_df = pd.DataFrame(input_data_dicts)

        customer_ids = input_df['customerID'].tolist()

        # Query to check if these CustomerIDs already exist
        query = f"""
            select * from past_predictions where past_predictions."customerID" in {tuple(customer_ids)}
        """
    else:
        input_data_dicts = [input_data_list.dict()]
        input_df = pd.DataFrame(input_data_dicts)

        customer_ids = input_df['customerID'].tolist()
        # print(customer_ids)

        # Query to check if these CustomerIDs already exist
        query = f"""
            select * from past_predictions where past_predictions."customerID" = '{customer_ids[0]}'
        """

    existing_records = await database.fetch_all(query)

    if existing_records:
        # Convert the results to a DataFrame
        existing_df = pd.DataFrame(existing_records)
        # print(existing_df)
        l = list(input_df.columns)
        l.append('prediction')
        # print(l)
        existing_df = existing_df[l]

        # Return a message with the existing CustomerIDs and associated data
        return {
            "message": "CustomerIDs already exist",
            "existing_data": True,
            "predictions": existing_df.to_dict(orient='records')
        }

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

    current_time = date.today()

    # Prepare the results for database insertion and return
    for idx, prediction_value in enumerate(predictions_values):
        input_data_dicts[idx]["prediction"] = int(prediction_value)
        input_data_dicts[idx]["date"] = current_time
        input_data_dicts[idx]["SourcePrediction"] = source_prediction

        query = predictions.insert().values(
            **input_data_dicts[idx]
        )
        await database.execute(query)

    return {"predictions": predictions_values} 


@app.get("/past_predictions/")
async def get_predictions(start_date: str = Query(None),
                          end_date: str = Query(None),
                          source: str = Query(None)):
    # Convert string dates to datetime objects
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if end_date:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    query = predictions.select()

    if start_date and end_date:
        query = query.where(
            and_(
                predictions.c.date >= start_date,
                predictions.c.date <= end_date
            )
        )
    if source and source != "All":
        query = query.where(predictions.c.SourcePrediction == source)

    results = await database.fetch_all(query)

    parsed_results = [dict(result) for result in results]

    return parsed_results

