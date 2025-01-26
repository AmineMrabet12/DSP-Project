import streamlit as st
import pandas as pd
import requests
import random
import os
import string
from dotenv import load_dotenv

load_dotenv()

# Load FastAPI URLs from environment variables
FASTAPI_PREDICT_URL = os.getenv('FASTAPI_PREDICT_URL')
FASTAPI_PAST_PREDICTIONS_URL = os.getenv('FASTAPI_PAST_PREDICTIONS_URL')

st.title("Prediction Web App") 

# Options for the user interface
option = st.selectbox("Select option:", ("Manual Input", "CSV File Upload", "View Past Predictions"))

def generate_unique_id():
    """Generate a unique ID for customerID."""
    letters = ''.join(random.choice(string.ascii_uppercase) for _ in range(4))
    numbers = str(random.randint(1000, 9999))
    return f"{numbers}-{letters}"

def generate_input_fields(df):
    """Generate input fields based on DataFrame columns."""
    input_data = {}
    for col in df.columns:
        col_type = df[col].dtype

        if col_type in ['float64', 'int64']:
            input_data[col] = st.selectbox(f"Select value for {col}", [0, 1] if col == 'SeniorCitizen' else [0.0])
        elif col_type == 'object':
            input_data[col] = generate_unique_id() if col == 'customerID' else st.selectbox(f"Select value for {col}", df[col].unique().tolist())
    return input_data

if option == "Manual Input":
    st.header("Input Features Manually")

    sample_df = pd.read_csv('../../data/churn.csv')
    sample_df['TotalCharges'] = pd.to_numeric(sample_df['TotalCharges'], errors='coerce')
    sample_df = sample_df.dropna()

    user_input = generate_input_fields(sample_df)

    if st.button("Predict"):
        response = requests.post(FASTAPI_PREDICT_URL, json=user_input, headers={"X-Source": "streamlit"})
        if response.status_code == 200:
            prediction = response.json()['predictions'][0]
            user_input['prediction'] = prediction
            st.success(f"Prediction: {prediction}")
            st.write(pd.DataFrame([user_input]).drop(columns=['Churn']))
        else:
            st.error("Error: Unable to get prediction")

elif option == "CSV File Upload":
    st.header("Upload CSV File")

    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    if uploaded_file:
        csv_data = pd.read_csv(uploaded_file)
        csv_data['TotalCharges'] = pd.to_numeric(csv_data['TotalCharges'], errors='coerce')
        csv_data = csv_data.dropna()

        st.write(csv_data)

        if st.button("Predict from CSV"):
            response = requests.post(FASTAPI_PREDICT_URL, json=csv_data.to_dict(orient='records'), headers={"X-Source": "streamlit"})
            if response.status_code == 200:
                result = response.json()
                if "existing_data" in result:
                    st.error("CustomerIDs already exist in the database:")
                    st.write(pd.DataFrame(result["predictions"]))
                else:
                    csv_data['Prediction'] = result["predictions"]
                    st.success("Predictions:")
                    st.write(csv_data.drop(columns=['Churn']))
            else:
                st.error("Error in processing the prediction request.")

elif option == "View Past Predictions":
    st.header("Past Predictions")

    # Date filter inputs
    col1, col2, col3 = st.columns(3)

    with col1:
        start_date = st.date_input("Start Date")
    with col2:
        end_date = st.date_input("End Date")
    with col3:
        source = st.selectbox("Select prediction source:", ("Web Application", "Scheduled Predictions", "All"))

    if start_date and end_date:
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source": source
        }
        response = requests.get(FASTAPI_PAST_PREDICTIONS_URL, params=params)
        
        if response.status_code == 200:
            past_predictions = response.json()
            if past_predictions:
                past_predictions_df = pd.DataFrame(past_predictions)
                st.write(past_predictions_df.drop(columns=['date', 'SourcePrediction']))
            else:
                st.write("No past predictions found for the selected date range.")
        else:
            st.error("Failed to fetch past predictions.")
