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

load_dotenv()

PATH_TO_MODULE = os.getenv('PATH_TO_MODULE')

sys.path.append(PATH_TO_MODULE)
import models

# Define paths
data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/")
RAW_DATA_PATH = os.path.join(data_path, "raw-data")
GOOD_DATA_PATH = os.path.join(data_path, "good-data")
BAD_DATA_PATH = os.path.join(data_path, "bad-data")

# Define the path to your Great Expectations project
GE_PROJECT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../gx/")

DATABASE_URL = os.getenv('DATABASE_URL')
WEBHOOK = os.getenv('WEBHOOK')
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

    # Task to read files
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
        for file_path in file_paths:
            # Load CSV data as a GE dataframe
            df = ge.read_csv(file_path)

            print(f"Validating file: {file_path}")

            data_asset_name = os.path.basename(file_path)

            # Load the Great Expectations context
            context = ge.data_context.DataContext(GE_PROJECT_PATH)

            batch_request = {
                # "datasource_name": "DSP-datasource",  # Update with your datasource
                # "data_connector_name": "default_inferred_data_connector_name",  # Ensure correct connector name
                "data_asset_name": data_asset_name,  # Dynamically pass the file name as data_asset_name
                # "data_connector_query": {"index": -1}
            }

            checkpoint_name = "DSP-validator"  # Update to your actual checkpoint name
            checkpoint_result = context.run_checkpoint(
                checkpoint_name=checkpoint_name,
                batch_request=batch_request
            )

            # print(checkpoint_result)
            run_results = checkpoint_result["run_results"]

            validation_key = next(iter(run_results))  # Since the key is dynamically generated

            validation_result = run_results[validation_key]["validation_result"]

            update_data_docs = run_results[validation_key]["actions_results"]["update_data_docs"]["local_site"]

            # Load the expectation suite
            expectation_suite = context.get_expectation_suite(expectation_suite_name="DSP-Suite")

            # Run the validation
            result = df.validate(expectation_suite, result_format="COMPLETE")

            context.build_data_docs()

            break

        return {
            "file_path": file_path, 
            "validation_result": validation_result,
            "update_data_docs": update_data_docs,
            "result": result
            }
    
    # Task to move the file (can be removed since we save files directly)
    @task
    def save_statistics(file_path, validation_result, results):
        data_asset_name = os.path.basename(file_path)
        # run_results = validation_result["run_results"]
        # validation_key = next(iter(run_results))  # Since the key is dynamically generated

        # validation_result = run_results[validation_key]["validation_result"]
        # validation_result = run_results[validation_key]["validation_result"]
        statistics_val = validation_result["statistics"]
        run_id = validation_result["meta"]["run_id"]
        datasource_name = validation_result["meta"]["active_batch_definition"]["datasource_name"]
        expectation_suite_name = validation_result["meta"]["expectation_suite_name"]
        checkpoint_name = validation_result["meta"]["checkpoint_name"]
        # file_name = validation_result["meta"]["batch_spec"]["path"]

        df = ge.read_csv(file_path)
        nb_rows = df.shape[0]
        nb_cols = df.shape[1]

        invalid_columns = set()
        valid_columns = set(df.columns)  # Assume all columns are valid initially

        # Analyze the validation results to find invalid columns
        for res in validation_result.results:
            column = res.expectation_config.kwargs.get("column")
            if column and not res.success:
                invalid_columns.add(column)
                if column in valid_columns:
                    valid_columns.remove(column)

        nb_invalid_cols = len(invalid_columns)
        nb_valid_cols = nb_cols - nb_invalid_cols

        unexpected_indexes = []
        for res in results.results:
            if "unexpected_index_list" in res.result:
                unexpected_indexes.extend(res.result["unexpected_index_list"])

        print(unexpected_indexes)

        invalid_rows = len(set(unexpected_indexes))
        print(invalid_rows)
        valid_rows = nb_rows - invalid_rows

        insert_data = {
            "run_id": run_id.run_name,
            "run_time": run_id.run_time,  # Get the run time from the run_id
            "evaluated_expectations": statistics_val['evaluated_expectations'],
            "successful_expectations": statistics_val['successful_expectations'],
            "unsuccessful_expectations": statistics_val['unsuccessful_expectations'],
            "success_percent": statistics_val['success_percent'],
            "datasource_name": datasource_name,
            "checkpoint_name": checkpoint_name,
            "expectation_suite_name": expectation_suite_name,
            "file_name": data_asset_name,
            "nb_rows": nb_rows,
            "nb_valid_rows": valid_rows,
            "nb_invalid_rows": invalid_rows,
            "nb_cols": nb_cols,
            "nb_valid_cols": nb_valid_cols,
            "nb_invalid_cols": nb_invalid_cols
        }

        # Insert the statistics into the database
        # with engine.begin() as connection:  # This will automatically commit and rollback
        #     insert_statement = insert(prediction_statistics_df).values(insert_data)  # Use the DataFrame structure
        #     connection.execute(insert_statement)
        with Session() as session:
            session.execute(insert(models.statistics).values(**insert_data))
            session.commit()

        print("Stat data inserted successfully!")

        # Print out the statistics
        print("Statistics from validation:")
        print(f"Evaluated expectations: {statistics_val['evaluated_expectations']}")
        print(f"Successful expectations: {statistics_val['successful_expectations']}")
        print(f"Unsuccessful expectations: {statistics_val['unsuccessful_expectations']}")
        print(f"Success percentage: {statistics_val['success_percent']}")

    @task
    def save_file(file_path, result):
        df = ge.read_csv(file_path)
        unexpected_indexes = []
        for res in result.results:
            if "unexpected_index_list" in res.result:
                unexpected_indexes.extend(res.result["unexpected_index_list"])

        # missing_value_indexes = df[df.isnull().any(axis=1)].index.tolist()
        unexpected_indexes = sorted(set(unexpected_indexes)) # + missing_value_indexes))

        if unexpected_indexes:
            unexpected_rows = df.iloc[unexpected_indexes]

            bad_file_path = os.path.join(BAD_DATA_PATH, f"bad_{os.path.basename(file_path)}")
            unexpected_rows.to_csv(bad_file_path, index=False)
            # print(unexpected_rows)
            print(unexpected_rows.shape)
            print(f"Saved unexpected rows to {bad_file_path}")

        # Save valid rows to the good data folder
        # else:
        good_rows = df.drop(index=unexpected_indexes)
        good_file_path = os.path.join(GOOD_DATA_PATH, f"good_{os.path.basename(file_path)}")
        good_rows.to_csv(good_file_path, index=False)
        # print(good_rows)
        print(good_rows.shape)
        print(f"Saved valid rows to {good_file_path}")

        os.remove(file_path)
        print(f"Processing completed for {file_path}")

    def send_teams_alert(webhook_url, message):
        payload = {
            "text": message
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
        
        if response.status_code != 200:
            raise ValueError(
                f"Request to Teams returned an error {response.status_code}, the response is:\n{response.text}"
            )
        
    def classify_criticality(validation_result):
        success_percent = validation_result["statistics"]["success_percent"]
        # unsuccessful_expectations = validation_result["statistics"]["unsuccessful_expectations"]

        if success_percent < 50: # or unsuccessful_expectations > 10:
            return "high"
        elif success_percent < 75:
            return "medium"
        else:
            return "low"
        
    @task
    def send_alerts(results, update_data_docs):
        criticality = classify_criticality(results)
        # print(results)
        suite_name = results["meta"]["expectation_suite_name"]
        file_name = os.path.basename(results["meta"]["batch_spec"]["path"])
        statistics_val = results["statistics"]
        # run_id = results["meta"]["run_id"]
        datasource_name = results["meta"]["active_batch_definition"]["datasource_name"]
        # expectation_suite_name = results["meta"]["expectation_suite_name"]
        checkpoint_name = results["meta"]["checkpoint_name"]

        # data_docs_url = results["update_data_docs"]["local_site"]

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
            f"**Find the data report here:** {update_data_docs}"
        )

            send_teams_alert(WEBHOOK, message)
        else:
            print(f"Validation successful for expectation suite {suite_name}.\nFile name: {file_name}")

    # Define task dependencies
    file_paths = read_data()
    results = validate_data(file_paths)  # Store the results
    send_alerts(results["validation_result"], results["update_data_docs"])
    save_statistics(results["file_path"], results["validation_result"], results['result'])  # Access file_path and validation_result
    save_file(results["file_path"], results['result'])

file_processing = file_processing_dag()
