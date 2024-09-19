from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
import os
import requests
import pandas as pd
import json


# Define folder paths
GOOD_DATA_FOLDER = "../data/good-data"

# Define the model API endpoint
MODEL_API_URL = "http://localhost:8000/predict"

PROCESSED_FILES_PATH = "../data/processed_files.json"

def load_processed_files():
    if os.path.exists(PROCESSED_FILES_PATH):
        try:
            with open(PROCESSED_FILES_PATH, 'r') as file:
                content = file.read().strip()
                if not content:  # Check if file is empty
                    return {}
                return json.loads(content)
        except json.JSONDecodeError:
            print("Error decoding JSON from processed files.")
            return {}
    return {}

def save_processed_files(files):    
    with open(PROCESSED_FILES_PATH, 'w') as file:
        json.dump(files, file)

# Define a function to check for new data
def check_for_new_data(**kwargs):
    files = os.listdir(GOOD_DATA_FOLDER)
    new_files = [f for f in files if os.path.isfile(os.path.join(GOOD_DATA_FOLDER, f)) and '.csv' in f]
    
    # Load previously processed files
    processed_files = load_processed_files()
    
    # Determine new files
    files_to_process = [f for f in new_files if f not in processed_files]
    
    if files_to_process:
        print('Found new data')
        print(files_to_process)
        # Pass the list of new files to the next task via XCom
        kwargs['ti'].xcom_push(key='new_files', value=files_to_process)
        
        # Update processed files list
        # save_processed_files(processed_files + files_to_process)
        processed_files.update({f: None for f in files_to_process})  # Use a placeholder for now
        save_processed_files(processed_files)
    else:
        # If no new files, skip the DAG run
        raise AirflowSkipException("No new files found in good_data folder")

# Define a function to make API call to the model service
def make_predictions(**kwargs):
    # Retrieve the list of new files from the previous task
    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='check_for_new_data')
    print(new_files)
    
    if new_files:
        for file in new_files:
            file_path = os.path.join(GOOD_DATA_FOLDER, file)

            df = pd.read_csv(file_path)

            data_json = df.to_dict(orient='records')

            predictions = []
    
            for row in data_json:
                response = requests.post(MODEL_API_URL, json=row)
                if response.status_code == 200:
                    predictions.append(response.json()['prediction'])
                else:
                    predictions.append("Error")

    return predictions


            # response = requests.post(MODEL_API_URL, json=data_json, timeout=30)

            # print(f"Prediction for {file}: {response.json()}")

            # with open(file_path, 'rb') as f:

            #     # Example request to the API with the file content
            #     response = requests.post(MODEL_API_URL, files={'file': f}, timeout=30)
            #     print(f"Prediction for {file}: {response.json()}")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 0,
}

# Define the DAG
with DAG(
    'check_and_predict',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Adjust this as needed
    catchup=False,
) as dag:

    # Task 1: Check for new data
    check_for_new_data_task = PythonOperator(
        task_id='check_for_new_data',
        python_callable=check_for_new_data,
        provide_context=True,
    )

    # Task 2: Make predictions
    make_predictions_task = PythonOperator(
        task_id='make_predictions',
        python_callable=make_predictions,
        provide_context=True,
    )

    # Set task dependencies
    check_for_new_data_task >> make_predictions_task
