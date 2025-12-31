import boto3 

def create_bucket(bucket_name):
    s3 = boto3.client('s3',
                  endpoint_url = 'minio:9000',
                  aws_access_key_id = 'admin',
                  aws_secret_access_key = 'admin123'
                )