from airflow import DAG
import psycopg2
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from random import randint
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
    # print(df)
    # return 'Done'
    # conn = psycopg2.connect(
    #     host="localhost",
    #     database="dsp",
    #     user="amine",
    #     password=""
    # )


    # query = "SELECT * FROM churn;"
    # df = pd.read_sql(query, con=conn)

    # # Close the connection
    # conn.close()

    # chunk_size = 10
    # num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)


    # for i in range(num_chunks):
    #     chunk = df[i * chunk_size:(i + 1) * chunk_size]
    #     chunk.to_csv(f'../data/splitted/chunk_{i+1}.csv', index=False) 

    # return num_chunks

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


def _training_model():
    return randint(1, 10)

def _split_data():
    df = pd.read_csv('../locs.csv')

    return df

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

        # training_model_A = PythonOperator(
        #     task_id="training_model_A",
        #     python_callable=_training_model
        # )

        # training_model_B = PythonOperator(
        #     task_id="training_model_B",
        #     python_callable=_training_model
        # )

        # training_model_C = PythonOperator(
        #     task_id="training_model_C",
        #     python_callable=_training_model
        # )

        # choose_best_model = BranchPythonOperator(
        #     task_id="choose_best_model",
        #     python_callable=_choose_best_model
        # )

        # accurate = BashOperator(
        #     task_id="accurate",
        #     bash_command="echo 'accurate'"
        # )

        # inaccurate = BashOperator(
        #     task_id="inaccurate",
        #     bash_command="echo 'inaccurate'"
        # )

        # grab_df = PythonOperator(
        #     task_id = 'testing_df',
        #     python_callable=_split_data
        # )

        # [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]
        connecting_to_df >> splitting