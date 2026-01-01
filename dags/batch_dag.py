from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from include.config.variables import BUCKETS
from plugins.operators.CreateBucketOperator import CreateBucketOperator
from datetime import datetime 

@dag(
    dag_id = "batch_pipeline",
    start_date = datetime(2021, 10, 10),
    catchup = False, 
    schedule = '@hourly'
)
def generate_dag():
    create_bronze = CreateBucketOperator(
        task_id = "create_bronze_layer",
        bucket_name = BUCKETS['bronze_layer']
    )

    create_silver = CreateBucketOperator(
        task_id = "create_silver_layer",
        bucket_name = BUCKETS['silver_layer']
    )

    create_gold = CreateBucketOperator(
        task_id = "create_gold_layer",
        bucket_name = BUCKETS['gold_layer']
    )

    create_bronze >> create_silver >> create_gold

generate_dag() 
