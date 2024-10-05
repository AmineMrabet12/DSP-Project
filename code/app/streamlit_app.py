import streamlit as st
import pandas as pd
import requests
import uuid
import random
import string

FASTAPI_PREDICT_URL = "http://localhost:8000/predict/"
FASTAPI_PAST_PREDICTIONS_URL = "http://localhost:8000/past_predictions/"

st.title("Prediction Web App")


option = st.selectbox("Select option:", ("Manual Input", "CSV File Upload", "View Past Predictions"))

def generate_unique_id():
    letters = ''.join(random.choice(string.ascii_uppercase) for _ in range(3))
    numbers = str(random.randint(100, 999))
    unique_id = f"{letters}-{numbers}"
    return unique_id

def generate_input_fields(df):
    input_data = {}
    for col in df.columns:
        col_type = df[col].dtype

        if col_type == 'float64' or col_type == 'int64':
            if col == 'SeniorCitizen':
                values = [0, 1]
                input_data[col] = st.selectbox(f"Select value for {col}", values)
            else:
                input_data[col] = st.number_input(f"Enter value for {col}", value=0.0)

        elif col_type == 'object':
            if col == 'customerID':
                input_data[col] = generate_unique_id()

                # input_data[col] = st.text_input(f"Enter value for {col}", placeholder='Id')

            elif col == 'Churn':
                pass 
            
            else:
                unique_values = df[col].unique().tolist()
                input_data[col] = st.selectbox(f"Select value for {col}", unique_values)


    return input_data


if option == "Manual Input":
    st.header("Input Features Manually")

    sample_df = pd.read_csv('../../data/churn.csv')
    sample_df['TotalCharges'] = pd.to_numeric(sample_df['TotalCharges'], errors='coerce')
    # sample_df['Churn'] = sample_df['Churn'].replace({'Yes': 1, 'No': 0})
    sample_df = sample_df.dropna()

    user_input = generate_input_fields(sample_df)

    if st.button("Predict"):
        response = requests.post(FASTAPI_PREDICT_URL, json=user_input, headers={"X-Source": "streamlit"})
        if response.status_code == 200:
            st.success(f"Prediction: {response.json()['predictions'][0]}")
        else:
            st.error("Error: Unable to get prediction")


elif option == "CSV File Upload":
    st.header("Upload CSV File")

    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    if uploaded_file is not None:
        csv_data = pd.read_csv(uploaded_file)
        csv_data['TotalCharges'] = pd.to_numeric(csv_data['TotalCharges'], errors='coerce')
        # csv_data['Churn'] = csv_data['Churn'].replace({'Yes': 1, 'No': 0})
        csv_data = csv_data.dropna()

        st.write(csv_data)

        if st.button("Predict from CSV"):
            data = csv_data.to_dict(orient='records')
            # predictions = []

            response = requests.post(FASTAPI_PREDICT_URL, json=data, headers={"X-Source": "streamlit"})

            # for row in data:
                # response = requests.post(FASTAPI_PREDICT_URL, json=row)
            if response.status_code == 200:
                predictions = response.json().get('predictions', [])
                csv_data['Prediction'] = predictions
                try:
                    st.write(csv_data.drop(columns=['Churn']))
                except:
                    st.write(csv_data)
            else:
                st.error("Error: Unable to get predictions")

            # csv_data['Prediction'] = predictions
            # st.write(csv_data)


elif option == "View Past Predictions":
    st.header("Past Predictions")

    # Date filter inputs
    col1, col2, col3 = st.columns(3)

    # Display start date in the first column
    with col1:
        start_date = st.date_input("Start Date")

    # Display end date in the second column
    with col2:
        end_date = st.date_input("End Date")

    with col3:
        source = st.selectbox("Select prediction source:", (
        "Web Application", "Scheduled Predictions", "All"))

    

    if start_date and end_date:
        # Send a request to FastAPI with date filters as query parameters
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source" : source
        }

        response = requests.get(FASTAPI_PAST_PREDICTIONS_URL, params=params)

        if response.status_code == 200:
            past_predictions = response.json()
            if len(past_predictions) > 0:
                past_predictions_df = pd.DataFrame(past_predictions)
                st.write(past_predictions_df.drop(columns=['date', 'SourcePrediction']))
            else:
                st.write("No past predictions found for the selected date range.")
        else:
            st.error("Failed to fetch past predictions.")
