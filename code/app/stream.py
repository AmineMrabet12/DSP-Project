import streamlit as st
import pandas as pd
import requests
from io import StringIO

# Set FastAPI endpoint URL
FASTAPI_PREDICT_URL = "http://localhost:8000/predict/"

# Streamlit UI
st.title("Prediction Web App")

# Option to choose input method
input_option = st.selectbox("Select input method:", ("Manual Input", "CSV File Upload"))

# Section for manual feature input
if input_option == "Manual Input":
    st.header("Input Features Manually")

    # Assuming your model expects these two features (expand based on your model)
    feature1 = st.number_input("Feature 1", min_value=0.0, value=0.0)
    feature2 = st.number_input("Feature 2", min_value=0.0, value=0.0)

    # Prediction button
    if st.button("Predict"):
        # Send request to FastAPI
        data = {
            "feature1": feature1,
            "feature2": feature2
        }

        response = requests.post(FASTAPI_PREDICT_URL, json=data)
        if response.status_code == 200:
            st.success(f"Prediction: {response.json()['prediction']}")
        else:
            st.error("Error: Unable to get prediction")

# Section for CSV file upload
elif input_option == "CSV File Upload":
    st.header("Upload CSV File")

    # Drag and drop area for file upload
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    if uploaded_file is not None:
        # Read the CSV
        csv_data = pd.read_csv(uploaded_file)

        # Display the CSV
        st.write(csv_data)

        # Ensure the file has the required columns for prediction
        if "feature1" in csv_data.columns and "feature2" in csv_data.columns:
            if st.button("Predict from CSV"):
                # Send CSV data to FastAPI for prediction
                data = csv_data.to_dict(orient='records')

                # Loop through each row and send a prediction request to FastAPI
                predictions = []
                for row in data:
                    response = requests.post(FASTAPI_PREDICT_URL, json=row)
                    if response.status_code == 200:
                        predictions.append(response.json()['prediction'])
                    else:
                        predictions.append("Error")

                # Display predictions
                csv_data['Prediction'] = predictions
                st.write(csv_data)

        else:
            st.error("CSV must contain 'feature1' and 'feature2' columns.")

