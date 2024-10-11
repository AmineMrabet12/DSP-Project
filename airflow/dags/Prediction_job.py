from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv

load_dotenv()

data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/")
GOOD_DATA_FOLDER = os.path.join(data_path, "good-data")
PROCESSED_FILES_PATH = os.path.join(data_path, "processed_files.json")

# MODEL_API_URL = "http://localhost:8000/predict"
MODEL_API_URL = os.getenv('FASTAPI_PREDICT_URL')

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


def check_for_new_data(**kwargs):
    files = os.listdir(GOOD_DATA_FOLDER)
    new_files = [f for f in files if os.path.isfile(os.path.join(GOOD_DATA_FOLDER, f)) and '.csv' in f]
    
    processed_files = load_processed_files()
    
    files_to_process = []
    for file in new_files:
        file_path = os.path.join(GOOD_DATA_FOLDER, file)
        last_mod_time = os.path.getmtime(file_path)
        
        if file not in processed_files or processed_files[file] != last_mod_time:
            files_to_process.append(file)
    
    if files_to_process:
        print('Found new or modified data')
        print(files_to_process)

        kwargs['ti'].xcom_push(key='new_files', value=files_to_process)
        
        for file in files_to_process:
            file_path = os.path.join(GOOD_DATA_FOLDER, file)
            processed_files[file] = os.path.getmtime(file_path)
        save_processed_files(processed_files)
    else:
        raise AirflowSkipException("No new or modified files found in good_data folder")


def make_predictions(**kwargs):
    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='check_for_new_data')
    print(new_files)
    
    if new_files:
        predictions = []
        for file in new_files:
            file_path = os.path.join(GOOD_DATA_FOLDER, file)

            df = pd.read_csv(file_path)

            data_json = df.to_dict(orient='records')

            # for row in data_json:
            # data_json["SourcePrediction"] = "Scheduled Predictions"

            response = requests.post(MODEL_API_URL, json=data_json, headers={"X-Source": "airflow"})


            # for row in data_json:
                # response = requests.post(MODEL_API_URL, json=row)
            if response.status_code == 200:
                if "message" in response.json():
                    predictions.append(response.json()['message']+' '+ f'for {file}')
                else:
                    predictions.append(response.json()['predictions'])
            else:
                predictions.append("Error")

    return predictions


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
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:

    check_for_new_data_task = PythonOperator(
        task_id='check_for_new_data',
        python_callable=check_for_new_data,
        provide_context=True,
    )

    make_predictions_task = PythonOperator(
        task_id='make_predictions',
        python_callable=make_predictions,
        provide_context=True,
    )

    check_for_new_data_task >> make_predictions_task
