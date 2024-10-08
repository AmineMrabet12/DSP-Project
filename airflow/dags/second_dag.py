from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
from airflow.exceptions import AirflowSkipException
# from airflow.provider.great_expectations.operators.great_expectations import GreatExpectationsOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Define paths
data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/")
RAW_DATA_PATH = os.path.join(data_path, "raw-data")
GOOD_DATA_PATH = os.path.join(data_path, "good-data")
BAD_DATA_PATH = os.path.join(data_path, "bad-data")

# Define the path to your Great Expectations project
GE_PROJECT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../great_expectations/")

@dag(
    dag_id='dsp_data_processing_2',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',
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

    # Task using GreatExpectationsOperator
    @task
    def validate_data(file_paths):
        for file_path in file_paths:
            data_asset_name = os.path.basename(file_path)

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

            with open('../great_expectations/checkpoints/test_validation_2.yml', 'w') as yaml_file:
                yaml_file.write(yaml_config)
                # print(f"Saved YAML configuration to {yaml_file_path}")
            
            # yaml = YAML()

            # Using GreatExpectationsOperator for validation
            validation_task = GreatExpectationsOperator(
                task_id=f'ge_validation_{data_asset_name}',
                checkpoint_name="test_validation_2",
                expectation_suite_name='validation_test',
                data_context_root_dir=GE_PROJECT_PATH,
                fail_task_on_validation_failure=False,
                return_json_dict=True
            )

            validation_task.execute(context={})  # Manually execute the operator
            break

        return file_path
    

    @task
    def save_file(file_path):
        # for file_path in file_paths:
        destination = os.path.join(GOOD_DATA_PATH, os.path.basename(file_path))
        shutil.move(file_path, destination)
        print(f"Moved {file_path} to {GOOD_DATA_PATH}")

    # Define task dependencies
    file_paths = read_data()
    file_path = validate_data(file_paths)
    save_file(file_path)

file_processing = file_processing_dag()
