from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, text
from datetime import timedelta
import logging

# Connection config
DB_CONN_ID = 'postgres_default'  # Change if using different Airflow connection ID
SQL_QUERY = "SELECT * FROM sellers LIMIT 10"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def read_postgres_table_sqlalchemy():
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(DB_CONN_ID)
    engine_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(engine_url)

    with engine.connect() as connection:
        result = connection.execute(text(SQL_QUERY))
        rows = result.fetchall()
        for row in rows:
            logging.info(row)

with DAG(
    dag_id='read_postgres_with_sqlalchemy',
    default_args=default_args,
    description='Reads a PostgreSQL table every 5min using SQLAlchemy',
    schedule_interval='*/5 * * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['postgres', 'sqlalchemy', 'frequent'],
    ) as dag:

    read_task = PythonOperator(
        task_id='read_postgres_task',
        python_callable=read_postgres_table_sqlalchemy,
    )

    read_task
