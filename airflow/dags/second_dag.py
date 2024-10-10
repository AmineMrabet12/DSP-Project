from airflow.decorators import dag, task
from datetime import datetime
import os
import shutil
import great_expectations as ge
from airflow.exceptions import AirflowSkipException
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
# from ruamel.yaml import YAML


# Define paths
data_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../data/")
RAW_DATA_PATH = os.path.join(data_path, "raw-data")
GOOD_DATA_PATH = os.path.join(data_path, "good-data")
BAD_DATA_PATH = os.path.join(data_path, "bad-data")

# Define the path to your Great Expectations project
GE_PROJECT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../gx/")

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
#     @task
#     def validate_data(file_paths):
#         for file_path in file_paths:
#             data_asset_name = os.path.basename(file_path)

#             checkpoint_name = 'DSP-validator'  # Update this to your actual checkpoint name
# #             yaml_config = f"""
# # name: {checkpoint_name}
# # config_version: 1.0
# # template_name:
# # module_name: great_expectations.checkpoint
# # class_name: Checkpoint
# # run_name_template: '%Y%m%d-%H%M%S-my-run-name-template'
# # expectation_suite_name: validation_test
# # batch_request: {{}}
# # action_list:
# #   - name: store_validation_result
# #     action:
# #       class_name: StoreValidationResultAction
# #   - name: store_evaluation_params
# #     action:
# #       class_name: StoreEvaluationParametersAction
# #   - name: update_data_docs
# #     action:
# #       class_name: UpdateDataDocsAction
# #       site_names: []
# # evaluation_parameters: {{}}
# # runtime_configuration: {{}}
# # validations:
# #   - batch_request:
# #       datasource_name: infected_second_test
# #       data_connector_name: default_inferred_data_connector_name
# #       data_asset_name: {data_asset_name}
# #     expectation_suite_name: validation_test
# #     result_format:
# #       result_format: COMPLETE
# #       include_unexpected_rows: True
# #       return_unexpected_index_query: True
# #       unexpected_index_query: True
# #       unexpected_index_list: True
# #       unexpected_list: True
# # profilers: []
# # ge_cloud_id:
# # expectation_suite_ge_cloud_id:
# # """

# #             with open('/Users/mohamedaminemrabet/Documents/EPITA/DSP/Final-Project-DSP/great_expectations/checkpoints/test_validation_2.yml', 'w') as yaml_file:
# #                 yaml_file.write(yaml_config)
#                 # print(f"Saved YAML configuration to {yaml_file_path}")

#             # yaml = YAML()
#             # data_asset_name = os.path.basename(file_path)
#             # print(f"Validating file: {file_path}")
#             # context = ge.data_context.DataContext(GE_PROJECT_PATH)
#             # checkpoint_result = context.run_checkpoint(
#             #     checkpoint_name=checkpoint_name
#             # )

#             # Using GreatExpectationsOperator for validation
#             validation_task = GreatExpectationsOperator(
#                 task_id=f'ge_validation_{data_asset_name}',
#                 checkpoint_name=checkpoint_name,
#                 expectation_suite_name='DSP-Suite',
#                 data_context_root_dir=GE_PROJECT_PATH,
#                 fail_task_on_validation_failure=False,
#                 return_json_dict=True
#             )

#             validation_task.execute(context={})  # Manually execute the operator
#             break

#         return file_path
    

#     @task
#     def save_file(file_path):
#         # for file_path in file_paths:
#         # destination = os.path.join(GOOD_DATA_PATH, os.path.basename(file_path))
#         # shutil.move(file_path, destination)
#         print(f"Moved {file_path} to {GOOD_DATA_PATH}")

    @task
    def validate_data(file_paths):
        for file_path in file_paths:
            # Load CSV data as a GE dataframe
            df = ge.read_csv(file_path)
            # df['TotalCharges'] = ge.to_numeric(df['TotalCharges'], errors='ignore')
            # df['MonthlyCharges'] = ge.to_numeric(df['MonthlyCharges'], errors='ignore')
            # df['tenure'] = ge.to_numeric(df['tenure'], errors='ignore')

            print(f"Validating file: {file_path}")

            # Load the Great Expectations context
            context = ge.data_context.DataContext(GE_PROJECT_PATH)

            # Load the expectation suite
            expectation_suite = context.get_expectation_suite(expectation_suite_name="DSP-Suite")

            # Run the validation
            result = df.validate(expectation_suite, result_format="COMPLETE")
            # print(result)

            # Extract unexpected row indexes
            unexpected_indexes = []
            for res in result.results:
                if "unexpected_index_list" in res.result:
                    unexpected_indexes.extend(res.result["unexpected_index_list"])

            # missing_value_indexes = df[df.isnull().any(axis=1)].index.tolist()
            unexpected_indexes = sorted(set(unexpected_indexes)) # + missing_value_indexes))

            if unexpected_indexes:
                # print(unexpected_indexes)
                # print(len(unexpected_indexes))
                unexpected_rows = df.iloc[unexpected_indexes]
                # print(f"Unexpected Rows:\n{unexpected_rows}")

                # Save the unexpected rows to the bad data folder
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

            context.build_data_docs()

            break

        return file_path
    
    # Task to move the file (can be removed since we save files directly)
    @task
    def save_file(file_path):
        os.remove(file_path)
        print(f"Processing completed for {file_path}")

    # Define task dependencies
    file_paths = read_data()
    file_path = validate_data(file_paths)
    save_file(file_path)

file_processing = file_processing_dag()
