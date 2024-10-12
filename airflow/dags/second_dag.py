from airflow.decorators import dag, task
from datetime import datetime
import os
import sys
import great_expectations as ge
from airflow.exceptions import AirflowSkipException
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up path to the module and append to system path
PATH_TO_MODULE = os.getenv('PATH_TO_MODULE')
sys.path.append(PATH_TO_MODULE)
import models

# Define paths
data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/")
RAW_DATA_PATH = os.path.join(data_path, "raw-data")
GOOD_DATA_PATH = os.path.join(data_path, "good-data")
BAD_DATA_PATH = os.path.join(data_path, "bad-data")
GE_PROJECT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../gx/")
DATABASE_URL = os.getenv('DATABASE_URL')
WEBHOOK = os.getenv('WEBHOOK')

# Set up database connection
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

@dag(
    dag_id='Data_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/1 * * * *',
    tags=['DSP'],
    catchup=False
)
def file_processing_dag():

    @task
    def read_data():
        raw_files = os.listdir(RAW_DATA_PATH)
        print(f"Files in {RAW_DATA_PATH}: {raw_files}")
        file_paths = [os.path.join(RAW_DATA_PATH, file) for file in raw_files if file.endswith('.csv')]

        if not file_paths:
            raise AirflowSkipException("No new data found to process.")

        return file_paths  

    @task(multiple_outputs=True)
    def validate_data(file_paths):
        validation_results = {}

        for file_path in file_paths:
            print(f"Validating file: {file_path}")
            df = ge.read_csv(file_path)
            data_asset_name = os.path.basename(file_path)
            context = ge.data_context.DataContext(GE_PROJECT_PATH)

            # Create batch request
            batch_request = {
                "data_asset_name": data_asset_name,
            }

            checkpoint_name = "DSP-validator"  # Update to your actual checkpoint name
            checkpoint_result = context.run_checkpoint(checkpoint_name=checkpoint_name, batch_request=batch_request)

            # Collect validation results
            run_results = checkpoint_result["run_results"]
            validation_key = next(iter(run_results))
            validation_result = run_results[validation_key]["validation_result"]
            update_data_docs = run_results[validation_key]["actions_results"]["update_data_docs"]["local_site"]

            # Validate the dataframe
            expectation_suite = context.get_expectation_suite(expectation_suite_name="DSP-Suite")
            result = df.validate(expectation_suite, result_format="COMPLETE")
            context.build_data_docs()

            validation_results[file_path] = {
                "validation_result": validation_result,
                "update_data_docs": update_data_docs,
                "result": result,
            }

        return validation_results
    
    @task
    def save_statistics(validation_results):
        with Session() as session:
            for file_path, results in validation_results.items():
                validation_result = results["validation_result"]
                statistics_val = validation_result["statistics"]
                run_id = validation_result["meta"]["run_id"]
                datasource_name = validation_result["meta"]["active_batch_definition"]["datasource_name"]
                expectation_suite_name = validation_result["meta"]["expectation_suite_name"]
                checkpoint_name = validation_result["meta"]["checkpoint_name"]

                df = ge.read_csv(file_path)
                nb_rows, nb_cols = df.shape
                valid_columns = set(df.columns)

                # Analyze validation results
                invalid_columns = {res.expectation_config.kwargs.get("column") for res in validation_result.results if not res.success}
                nb_invalid_cols = len(invalid_columns)
                nb_valid_cols = nb_cols - nb_invalid_cols

                unexpected_indexes = [idx for res in results['result'].results if "unexpected_index_list" in res.result for idx in res.result["unexpected_index_list"]]
                invalid_rows = len(set(unexpected_indexes))
                valid_rows = nb_rows - invalid_rows

                insert_data = {
                    "run_id": run_id.run_name,
                    "run_time": run_id.run_time,
                    "evaluated_expectations": statistics_val['evaluated_expectations'],
                    "successful_expectations": statistics_val['successful_expectations'],
                    "unsuccessful_expectations": statistics_val['unsuccessful_expectations'],
                    "success_percent": statistics_val['success_percent'],
                    "datasource_name": datasource_name,
                    "checkpoint_name": checkpoint_name,
                    "expectation_suite_name": expectation_suite_name,
                    "file_name": os.path.basename(file_path),
                    "nb_rows": nb_rows,
                    "nb_valid_rows": valid_rows,
                    "nb_invalid_rows": invalid_rows,
                    "nb_cols": nb_cols,
                    "nb_valid_cols": nb_valid_cols,
                    "nb_invalid_cols": nb_invalid_cols
                }

                session.execute(insert(models.statistics).values(**insert_data))
            session.commit()

        print("Statistics data inserted successfully!")

    @task
    def save_file(validation_results):
        for file_path, results in validation_results.items():
            df = ge.read_csv(file_path)
            unexpected_indexes = [idx for res in results['result'].results if "unexpected_index_list" in res.result for idx in res.result["unexpected_index_list"]]
            unexpected_indexes = sorted(set(unexpected_indexes))

            if unexpected_indexes:
                unexpected_rows = df.iloc[unexpected_indexes]
                bad_file_path = os.path.join(BAD_DATA_PATH, f"bad_{os.path.basename(file_path)}")
                unexpected_rows.to_csv(bad_file_path, index=False)
                print(f"Saved unexpected rows to {bad_file_path}")

            good_rows = df.drop(index=unexpected_indexes)
            good_file_path = os.path.join(GOOD_DATA_PATH, f"good_{os.path.basename(file_path)}")
            good_rows.to_csv(good_file_path, index=False)
            print(f"Saved valid rows to {good_file_path}")

            os.remove(file_path)
            print(f"Processing completed for {file_path}")

    def send_teams_alert(webhook_url, message):
        payload = {"text": message}
        headers = {"Content-Type": "application/json"}
        response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
        
        if response.status_code != 200:
            raise ValueError(f"Request to Teams returned an error {response.status_code}, the response is:\n{response.text}")
        
    def classify_criticality(validation_result):
        success_percent = validation_result["statistics"]["success_percent"]
        return "high" if success_percent < 50 else "medium" if success_percent < 75 else "low"
        
    @task
    def send_alerts(validation_results):
        for file_path, results in validation_results.items():
            criticality = classify_criticality(results)
            suite_name = results["meta"]["expectation_suite_name"]
            file_name = os.path.basename(results["meta"]["batch_spec"]["path"])
            statistics_val = results["statistics"]
            datasource_name = results["meta"]["active_batch_definition"]["datasource_name"]
            checkpoint_name = results["meta"]["checkpoint_name"]

            if not results["success"]:
                message = (
                    f"**Validation failed for expectation suite:** {suite_name}\n\n"
                    f"**Criticality:** {criticality}\n\n"
                    f"**File name:** {file_name}\n\n"
                    f"**Datasource:** {datasource_name}\n\n"
                    f"**Checkpoint:** {checkpoint_name}\n\n"
                    f"**Evaluated expectations:** {statistics_val['evaluated_expectations']}\n\n"
                    f"**Successful expectations:** {statistics_val['successful_expectations']}\n\n"
                    f"**Unsuccessful expectations:** {statistics_val['unsuccessful_expectations']}\n\n"
                    f"**Success percentage:** {statistics_val['success_percent']}%\n\n"
                )
                send_teams_alert(WEBHOOK, message)
            else:
                print(f"Validation successful for expectation suite {suite_name}.\nFile name: {file_name}")

    # Define task dependencies
    file_paths = read_data()
    validation_results = validate_data(file_paths)
    save_statistics(validation_results)
    send_alerts(validation_results)
    save_file(validation_results)

file_processing = file_processing_dag()
