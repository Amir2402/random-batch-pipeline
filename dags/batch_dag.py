from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from include.config.variables import BUCKETS, API_URL, USER_DATA, SLACK_API_KEY, CHANNEL_ID, BOT_NAME
from include.utils.queries import select_user_dimension, select_location_dimension, select_date_dimension
from plugins.operators.CreateBucketOperator import CreateBucketOperator
from plugins.operators.bronze.LoadApiDataToBronze import LoadUserDataToBronze
from plugins.operators.quality.ApiInputValidator import ApiInputValidator
from plugins.operators.quality.SlackNotifier import SlackNotifier
from plugins.operators.silver.ProcessUserData import ProcessUserData
from plugins.operators.gold.LoadDimToGold import LoadDimToGold
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
        file_name = USER_DATA,
        api_url = API_URL,
        now_timestamp = now
    )

    validate_user_data_schema = ApiInputValidator(
        task_id = 'validate_user_data_schema',
        bucket_name = BUCKETS['bronze_layer'],
        now_timestamp = now,
        file_name = USER_DATA
    )

    alert_slack_schema_change = SlackNotifier(
        task_id = "alert_slack_schema_change",
        trigger_rule = "one_failed",
        channel_id = CHANNEL_ID,
        bot_name = BOT_NAME,
        slack_token = SLACK_API_KEY,
        now_timestamp = now
    )
    
    process_silver_user_data = ProcessUserData(
        task_id = 'process_silver_user_data',
        bucket_name = BUCKETS['silver_layer'],
        table_name = 'user_data',
        now_timestamp = now 
    )

    load_user_dim = LoadDimToGold(
        task_id = 'load_user_dimension',
        delta_table_name = 'silver_user_data',
        duckdb_table_name = 'user_dim',
        input_bucket_name = BUCKETS['silver_layer'], 
        output_bucket_name = BUCKETS['gold_layer'],
        query = select_user_dimension,
        now_timestamp = now
    )

    load_location_dim = LoadDimToGold(
        task_id = 'load_location_dimension',
        delta_table_name = 'silver_user_data',
        duckdb_table_name = 'location_dim',
        input_bucket_name = BUCKETS['silver_layer'], 
        output_bucket_name = BUCKETS['gold_layer'],
        query = select_location_dimension,
        now_timestamp = now
    )

    load_date_dim = LoadDimToGold(
        task_id = 'load_date_dimension',
        delta_table_name = 'silver_user_data',
        duckdb_table_name = 'date_dim',
        input_bucket_name = BUCKETS['silver_layer'], 
        output_bucket_name = BUCKETS['gold_layer'],
        query = select_date_dimension,
        now_timestamp = now
    )

    [create_bronze, create_silver, create_gold] >> load_user_data_to_bronze >> validate_user_data_schema
    validate_user_data_schema >> alert_slack_schema_change
    validate_user_data_schema >> process_silver_user_data >> [load_user_dim, load_location_dim, load_date_dim]

generate_dag() 
