from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
import pandas as pd
from airflow.exceptions import AirflowSkipException
import great_expectations as ge

# Define paths
data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/")
RAW_DATA_PATH = os.path.join(data_path, "raw-data")
GOOD_DATA_PATH = os.path.join(data_path, "good-data")
BAD_DATA_PATH = os.path.join(data_path, "bad-data")

# Define the path to your Great Expectations project
GE_PROJECT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../great_expectations/")

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
        print(f"Files in {RAW_DATA_PATH}: {raw_files}")
        
        # Get CSV file paths from the raw folder
        file_paths = [os.path.join(RAW_DATA_PATH, file) for file in raw_files if file.endswith('.csv')]

        # If no new files, raise AirflowSkipException to mark the task as skipped
        if not file_paths:
            raise AirflowSkipException("No new data found to process.")
        
        return file_paths  

    @task
    def validate_data(file_paths):
        context = ge.data_context.DataContext(GE_PROJECT_PATH)
        valid_files = []
        invalid_files = []

        # Load the Expectation Suite
        expectation_suite = context.get_expectation_suite(expectation_suite_name="validation_test")

        for file_path in file_paths:
            data_asset_name = os.path.basename(file_path)

            # Load the file as a DataFrame
            df = pd.read_csv(file_path)
            batch = ge.dataset.PandasDataset(df)

            # Validate the DataFrame against the loaded Expectation Suite
            result = batch.validate(expectation_suite=expectation_suite)

            invalid_indices = set()

            # Loop through validation results to collect invalid row indices
            for validation in result['results']:
                if not validation['success']:
                    # Collect the list of invalid row indices for the failed expectations
                    unexpected_indices = validation['result'].get('unexpected_index_list', [])
                    invalid_indices.update(unexpected_indices)

            # Separate valid and invalid rows
            valid_rows_df = df.drop(list(invalid_indices))
            invalid_rows_df = df.iloc[list(invalid_indices)]

            if not valid_rows_df.empty:
                valid_files.append((valid_rows_df, file_path))
            if not invalid_rows_df.empty:
                invalid_files.append((invalid_rows_df, file_path))

            break

        return {"valid": valid_files, "invalid": invalid_files}

    @task
    def save_file(validation_result):
        # Save valid rows to the good-data folder
        for valid_data, original_file in validation_result['valid']:
            destination = os.path.join(GOOD_DATA_PATH, os.path.basename(original_file))
            valid_data.to_csv(destination, index=False)
            print(f"Saved valid data to {destination}")

        # Save invalid rows to the bad-data folder
        for invalid_data, original_file in validation_result['invalid']:
            destination = os.path.join(BAD_DATA_PATH, os.path.basename(original_file))
            invalid_data.to_csv(destination, index=False)
            print(f"Saved invalid data to {destination}")

    # Define task dependencies
    file_paths = read_data()
    validation_result = validate_data(file_paths)
    save_file(validation_result)

file_processing = file_processing_dag()
