import streamlit as st
import pandas as pd
import requests

# Set FastAPI endpoints
FASTAPI_PREDICT_URL = "http://localhost:8000/predict/"
FASTAPI_PAST_PREDICTIONS_URL = "http://localhost:8000/past_predictions/"

# Streamlit UI
st.title("Prediction Web App")

# Option to choose input method or view past predictions
option = st.selectbox("Select option:", ("Manual Input", "CSV File Upload", "View Past Predictions"))

# Define a function to generate input fields based on data types
def generate_input_fields(df):
    input_data = {}
    for col in df.columns:
        col_type = df[col].dtype

        if col_type == 'float64' or col_type == 'int64':
            input_data[col] = st.number_input(f"Enter value for {col}", value=0.0)

        elif col_type == 'object':
            if col == 'customerID':
                input_data[col] = st.text_input(f"Enter value for {col}", placeholder='Id')

            elif col == 'Churn':
                pass 
            
            else:
                # Get unique values in the object column for dropdown options
                unique_values = df[col].unique().tolist()
                input_data[col] = st.selectbox(f"Select value for {col}", unique_values)

    return input_data

# Section for manual feature input
if option == "Manual Input":
    st.header("Input Features Manually")

    sample_df = pd.read_csv('../../data/churn.csv')
    sample_df['TotalCharges'] = pd.to_numeric(sample_df['TotalCharges'], errors='coerce')
    # sample_df['Churn'] = sample_df['Churn'].replace({'Yes': 1, 'No': 0})
    sample_df = sample_df.dropna()

    # Generate input fields dynamically
    user_input = generate_input_fields(sample_df)

    if st.button("Predict"):
        # Send request to FastAPI
        response = requests.post(FASTAPI_PREDICT_URL, json=user_input)
        if response.status_code == 200:
            st.success(f"Prediction: {response.json()['prediction']}")
        else:
            st.error("Error: Unable to get prediction")

# Section for CSV file upload
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
            predictions = []
    
            for row in data:
                response = requests.post(FASTAPI_PREDICT_URL, json=row)
                if response.status_code == 200:
                    predictions.append(response.json()['prediction'])
                else:
                    predictions.append("Error")

            csv_data['Prediction'] = predictions
            st.write(csv_data)

# Section to view past predictions
elif option == "View Past Predictions":
    st.header("Past Predictions")

    # Fetch past predictions from FastAPI
    response = requests.get(FASTAPI_PAST_PREDICTIONS_URL)

    if response.status_code == 200:
        past_predictions = response.json()
        if len(past_predictions) > 0:
            # Convert to DataFrame for better display
            past_predictions_df = pd.DataFrame(past_predictions)
            st.write(past_predictions_df)
        else:
            st.write("No past predictions found.")
    else:
        st.error("Failed to fetch past predictions.")
