import joblib
from fastapi import FastAPI

app = FastAPI()

# model = joblib.load("ai_app/models/model.joblib")


@app.post("/predict")
def predict():
    return {"name": "loulou",
            "secret_message": "youyou"}
    # Preprocess the features and predict
    # prediction = model.predict([list(features.values())])
    # return {"prediction": prediction[0]}
