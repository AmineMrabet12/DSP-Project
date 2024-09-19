from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago
import os
import requests


# Define folder paths
GOOD_DATA_FOLDER = "../data/good-data/"

# Define the model API endpoint
MODEL_API_URL = "http://localhost:8000/predict"

# Define a function to check for new data
def check_for_new_data(**kwargs):
    files = os.listdir(GOOD_DATA_FOLDER)
    new_files = [f for f in files if os.path.isfile(os.path.join(GOOD_DATA_FOLDER, f))]
    
    if new_files:
        # Pass the list of new files to the next task via XCom
        kwargs['ti'].xcom_push(key='new_files', value=new_files)
    else:
        # If no new files, skip the DAG run
        raise AirflowSkipException("No new files found in good_data folder")

# Define a function to make API call to the model service
def make_predictions(**kwargs):
    # Retrieve the list of new files from the previous task
    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='check_for_new_data')
    
    if new_files:
        for file in new_files:
            file_path = os.path.join(GOOD_DATA_FOLDER, file)
            with open(file_path, 'rb') as f:
                # Example request to the API with the file content
                response = requests.post(MODEL_API_URL, files={'file': f})
                print(f"Prediction for {file}: {response.json()}")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'check_and_predict',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust this as needed
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
