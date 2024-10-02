from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime


def _read_data():
    df = pd.read_csv('../data/churn.csv')


    return df
    # df.to_csv('test.csv', index=False)

def split_data(df):
    chunk_size = 10
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)


    for i in range(num_chunks):
        chunk = df[i * chunk_size:(i + 1) * chunk_size]
        chunk.to_csv(f'data/splitted/chunk_{i+1}.csv', index=False) 

    return 'Fiels are saved'


with DAG("fetching_postgres", start_date=datetime(2024, 1, 1),
    schedule="@daily", catchup=False) as dag:

        connecting_to_df = PythonOperator(
             task_id='connecting_to_df',
             python_callable=_read_data
        )

        splitting = PythonOperator(
             task_id='splitting_data',
             python_callable= split_data
        )

        connecting_to_df >> splitting