# air_ingest.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='run_postgres_ingest_script',
    default_args=default_args,
    description='Runs ingest.py every 5 minutes',
    schedule_interval='*/5 * * * *',
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['ingestion'],
) as dag:

    run_ingest = BashOperator(
        task_id='run_ingest_script',
        bash_command='python3 /home/k13104/Desktop/Programming/Test_brazil/airflow/scripts/ingest.py >> /tmp/ingest_log.txt 2>&1',
        execution_timeout=timedelta(minutes=4),
    )

    run_ingest