from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import pickle  # assuming you saved your model as a pickle file
import pandas as pd  # to format input data
from pydantic import BaseModel
from .models import SessionLocal, Prediction

app = FastAPI()

# Dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Model input schema
class ModelInput(BaseModel):
    feature1: float
    feature2: float
    # Add more features as per your model

# Load your trained model
with open("path_to_model/model.pkl", "rb") as f:
    model = pickle.load(f)

# Endpoint 1: Predict and save the result
@app.post("/predict/")
def predict(input_data: ModelInput, db: Session = Depends(get_db)):
    input_df = pd.DataFrame([input_data.dict()])
    prediction = model.predict(input_df)[0]  # Assuming it's a regression model

    # Save the input and prediction to the database
    db_prediction = Prediction(input_data=str(input_data.dict()), prediction=prediction)
    db.add(db_prediction)
    db.commit()
    db.refresh(db_prediction)

    return {"input": input_data, "prediction": prediction}

# Endpoint 2: Get past predictions
@app.get("/predictions/")
def get_predictions(db: Session = Depends(get_db)):
    predictions = db.query(Prediction).all()
    return predictions
