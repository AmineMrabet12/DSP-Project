from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
from airflow.exceptions import AirflowSkipException
import great_expectations as ge
# from ruamel.yaml import YAML

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
    schedule_interval='*/1 * * * *',
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

        for file_path in file_paths:
            data_asset_name = os.path.basename(file_path)

            # Create a batch request for the file
            # batch_request = {
            #     # 'datasource_name': 'infected_second_test',  # Update these with your Great Expectations setup
            #     # 'data_connector_name': 'default_inferred_data_connector_name',
            #     # 'data_asset_name': data_asset_name,  # Use the filename as the data asset name
            #     # 'runtime_parameters': {'batch_data': file_path},  # Pass the file path dynamically
            #     # 'batch_spec_passthrough': {'reader_options': {'header': True}},
            #     # 'batch_identifiers': {'default_identifier': 'default'}  # Adding batch_identifiers
            # }
            
            # Run the validation
            checkpoint_name = 'test_validation_2'  # Update this to your actual checkpoint name
            yaml_config = f"""
name: {checkpoint_name}
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
expectation_suite_name: validation_test
batch_request: {{}}
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
evaluation_parameters: {{}}
runtime_configuration: {{}}
validations:
  - batch_request:
      datasource_name: infected_second_test
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: {data_asset_name}
    expectation_suite_name: validation_test
profilers: []
ge_cloud_id:
expectation_suite_ge_cloud_id:
"""

            with open('/Users/wasedoo/Documents/EPITA/M2/DSP/DSP-Project/great_expectations/checkpoints/test_validation_2.yml', 'w') as yaml_file:
                yaml_file.write(yaml_config)
                # print(f"Saved YAML configuration to {yaml_file_path}")
            
            # yaml = YAML()
            # context.add_checkpoint(**yaml.load(yaml_config))

            result = context.run_checkpoint(
                checkpoint_name=checkpoint_name
                # batch_request=batch_request
                # expectation_suite_name="validation_test",
                # validations=[
                #     {
                #         "batch_request": batch_request
                #     }
                # ]
            )

            # Check if the validation passed
            if result.success:
                print(f"{file_path} passed validation.")
                valid_files.append(file_path)
            else:
                print(f"{file_path} failed validation.")
                invalid_files.append(file_path)

            break

        return {"valid": valid_files, "invalid": invalid_files}


    @task
    def save_file(validation_result):
        # Move valid files to the good-data folder
        for valid_file in validation_result['valid']:
            destination = os.path.join(GOOD_DATA_PATH, os.path.basename(valid_file))
            if os.path.exists(destination):
                os.remove(destination)
            shutil.move(valid_file, GOOD_DATA_PATH)
            print(f"Moved {valid_file} to {GOOD_DATA_PATH}")

        # Move invalid files to the bad-data folder
        for invalid_file in validation_result['invalid']:
            destination = os.path.join(BAD_DATA_PATH, os.path.basename(invalid_file))
            if os.path.exists(destination):
                os.remove(destination)
            shutil.move(invalid_file, BAD_DATA_PATH)
            print(f"Moved {invalid_file} to {BAD_DATA_PATH}")

    # Define task dependencies
    file_paths = read_data()
    validation_result = validate_data(file_paths)
    save_file(validation_result)

file_processing = file_processing_dag()
