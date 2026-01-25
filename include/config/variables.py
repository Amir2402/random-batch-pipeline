import os

BUCKETS = {
    'bronze_layer': os.getenv('BRONZE_LAYER'),
    'silver_layer': os.getenv('SILVER_LAYER'),
    'gold_layer': os.getenv('GOLD_LAYER')
}

S3_ACCESS = {
    's3_endpoint': os.getenv('S3_ENDPOINT'),
    'aws_access_key': os.getenv('AWS_ACCESS_KEY'),
    'aws_secret_key': os.getenv('AWS_SECRET_KEY'),
    'region_name': os.getenv('REGION_NAME'),
    's3_endpoint_duckdb': os.getenv('S3_ENDPOINT_DUCKDB')
}

API_URL = os.getenv('API_URL')
USER_DATA = os.getenv('USER_DATA')

SLACK_API_KEY = os.getenv('SLACK_API_KEY')

CHANNEL_ID = os.getenv('CHANNEL_ID')
BOT_NAME = "airflow notifier"