from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
import pandas as pd

# Define paths for raw and good data
RAW_DATA_PATH = '/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/data/raw-data'
GOOD_DATA_PATH = '/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/data/good-data'

# Define the DAG using the @dag decorator
@dag(
    dag_id='dsp_data_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    tags=['DSP'],
    catchup=False
)

def file_processing_dag():

    @task
    def read_data():
        raw_files = os.listdir(RAW_DATA_PATH)
        file_paths = [os.path.join(RAW_DATA_PATH, file) for file in raw_files if file.endswith('.csv')]

        return file_paths  

    @task
    def save_file(file_paths):
        for file in file_paths:
            shutil.move(file, GOOD_DATA_PATH)

    file_paths = read_data()
    save_file(file_paths)


file_processing = file_processing_dag()
