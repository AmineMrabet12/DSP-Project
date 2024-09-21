from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from database import database, engine, metadata
from models import predictions
from joblib import load


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
async def predict(input_data_: ModelInput):

        input_data_dict = input_data_.dict()


        input_df = pd.DataFrame([input_data_dict])

        ordinal = load('../../models/Ordinal_Encoder.joblib')
        scaler = load('../../models/Standard_Scaler.joblib')
        categorical_columns = load('../../models/categorical_columns.joblib')
        columns = load('../../models/columns.joblib')

        input_df = input_df[columns]

        input_df[categorical_columns] = ordinal.transform(input_df[categorical_columns])
        input_df = scaler.transform(input_df)

        prediction_value = model.predict(input_df)[0].item()
        print(prediction_value)

        # combined_data = input_data_.dict()
        combined_data = input_data_dict

        combined_data["prediction"] = int(prediction_value)

        query = predictions.insert().values(
                # json.dumps(combined_json)
                **combined_data
        )

        await database.execute(query)

        return {"input": input_data_dict, "prediction": prediction_value}


@app.get("/past_predictions/")
async def get_predictions():
        query = predictions.select()
        results = await database.fetch_all(query)

        parsed_results = [
                dict(result)
                for result in results
        ]

        return parsed_results
