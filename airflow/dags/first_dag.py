from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
from airflow.exceptions import AirflowSkipException


RAW_DATA_PATH = '/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/data/raw-data'
GOOD_DATA_PATH = '/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/data/good-data'


@dag(
    dag_id='dsp_data_processing',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/1 * * * *',
    tags=['DSP'],
    catchup=False
)

def file_processing_dag():
    
    @task
    def read_data():
        raw_files = os.listdir(RAW_DATA_PATH)
        # print(raw_files)
        # print(RAW_DATA_PATH)
        # file_paths = [os.path.join(RAW_DATA_PATH, file) for file in raw_files if file.endswith('.csv')]

        # return file_paths 
        # raw_files = os.listdir(RAW_DATA_PATH)
        print(f"Files in {RAW_DATA_PATH}: {raw_files}")
        
        # Get CSV file paths from the raw folder
        file_paths = [os.path.join(RAW_DATA_PATH, file) for file in raw_files if file.endswith('.csv')]

        # If no new files, raise AirflowSkipException to mark the task as skipped
        if not file_paths:
            raise AirflowSkipException("No new data found to process.")
        
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
            break

    file_paths = read_data()
    save_file(file_paths)


file_processing = file_processing_dag()
