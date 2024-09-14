import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'postgres_to_csv_dag',
    default_args=default_args,
    description='A DAG that reads data from Postgres, splits it, and saves to CSV',
    schedule_interval=None,  # Change to your preferred schedule
)

# Define the folder where CSVs will be saved
output_folder = 'data/splitted'

# Ensure the folder exists
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def fetch_and_split_data():
    # Connect to the PostgreSQL database using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='dsp_testing')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Execute your query
    cursor.execute("SELECT * FROM churn;")
    data = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    # Convert the data into a Pandas DataFrame
    df = pd.DataFrame(data, columns=colnames)

    # Split the DataFrame into chunks of 10 rows each
    chunk_size = 10
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)

    for i in range(num_chunks):
        chunk = df[i * chunk_size:(i + 1) * chunk_size]
        chunk.to_csv(os.path.join(output_folder, f'chunk_{i+1}.csv'), index=False)

# Define the PythonOperator to run the function
split_data_task = PythonOperator(
    task_id='fetch_and_split_data',
    python_callable=fetch_and_split_data,
    dag=dag,
)

# Set the task in the DAG
split_data_task
