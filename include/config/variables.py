import os

BUCKETS = {
    'bronze_layer': 'bronze',
    'silver_layer': 'silver',
    'gold_layer': 'gold'
}

S3_ACCESS = {
    's3_endpoint': 'http://minio:9000',
    'aws_access_key': 'admin',
    'aws_secret_key': 'admin123',
    'region_name': 'us-east-1',
    's3_endpoint_duckdb': 'minio:9000'
}

API_URL = 'https://randomuser.me/api/'
USER_DATA = 'user_data'

SLACK_API_KEY = os.getenv('SLACK_API_KEY')

CHANNEL_ID = "all-airflow" 
BOT_NAME = "airflow notifier"