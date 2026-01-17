from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from include.config.variables import BUCKETS, API_URL
from plugins.operators.CreateBucketOperator import CreateBucketOperator
from plugins.operators.bronze.LoadApiDataToBronze import LoadUserDataToBronze
from datetime import datetime 

now = datetime.now()

@dag(
    dag_id = "batch_pipeline",
    start_date = datetime(2021, 10, 10),
    catchup = False, 
    schedule = '@hourly'
)
def generate_dag():
    create_bronze = CreateBucketOperator(
        task_id = "create_bronze_layer",
        bucket_name = BUCKETS['bronze_layer'],
        now_timestamp = now
    )

    create_silver = CreateBucketOperator(
        task_id = "create_silver_layer",
        bucket_name = BUCKETS['silver_layer'],
        now_timestamp = now
    )

    create_gold = CreateBucketOperator(
        task_id = "create_gold_layer",
        bucket_name = BUCKETS['gold_layer'],
        now_timestamp = now
    )

    load_user_data_to_bronze = LoadUserDataToBronze(
        task_id = 'load_API_data_to_bronze',
        bucket_name = BUCKETS['bronze_layer'],
        api_url = API_URL,
        now_timestamp = now
    ) 

    [create_bronze, create_silver, create_gold] >> load_user_data_to_bronze

generate_dag() 
