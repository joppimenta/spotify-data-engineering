from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.expanduser("~/projects/spotify-project/spotify-extract-data"))
from main import main as extract_data  # main para extrair os dados da api do spotify


default_args = {
    'owner': 'joao',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_pipeline',
    default_args=default_args,
    description='Pipeline que extrai dados do Spotify e executa DBT',
    schedule_interval='@daily',  # executa diariamente
    start_date=datetime(2025, 10, 26),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_spotify_data',
        python_callable=extract_data
    )

    dbt_task = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd ~/projects/spotify-project/spotify-dbt/spotify_project && '
            'dbt run --profiles-dir .'
        )
    )

    extract_task >> dbt_task  # define a ordem das tarefas