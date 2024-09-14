from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import random

def _choose_path():
    paths = ['path_a', 'path_b']
    return random.choice(paths)  # Randomly choose between two tasks

def _print_a():
    print("Task A was selected")

def _print_b():
    print("Task B was selected")

with DAG(dag_id='branching_dag', start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:

    choose_task = BranchPythonOperator(
        task_id='choose_path',
        python_callable=_choose_path
    )

    task_a = PythonOperator(
        task_id='path_a',
        python_callable=_print_a
    )

    task_b = PythonOperator(
        task_id='path_b',
        python_callable=_print_b
    )

    choose_task >> [task_a, task_b]
