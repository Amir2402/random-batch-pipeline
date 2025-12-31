from airflow.decorators import DAG
from airflow.operators.python import PythonOperator
from include.create_buckets import create_bucket
from datetime import datetime 

@DAG(
    dag_id = "batch_pipeline",
    start_date = datetime(2021, 10, 10),
    catchup = False, 
    schedule = '@hourly'
)
def generate_dag():
    pass