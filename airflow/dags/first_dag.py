from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
import pandas as pd


RAW_DATA_PATH = '/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/data/raw-data'
GOOD_DATA_PATH = '/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/data/good-data'


@dag(
    dag_id='dsp_data_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',
    tags=['DSP'],
    catchup=False
)

def file_processing_dag():
    
    @task
    def read_data():
        raw_files = os.listdir(RAW_DATA_PATH)
        print(raw_files)
        print(RAW_DATA_PATH)
        file_paths = [os.path.join(RAW_DATA_PATH, file) for file in raw_files if file.endswith('.csv')]

        return file_paths  

    @task
    def save_file(file_paths):
        for file in file_paths:
            # try:
            destination = os.path.join(GOOD_DATA_PATH, os.path.basename(file))

            if os.path.exists(destination):
                os.remove(destination)

            shutil.move(file, GOOD_DATA_PATH)
            # except:
            #     pass

    file_paths = read_data()
    save_file(file_paths)


file_processing = file_processing_dag()
