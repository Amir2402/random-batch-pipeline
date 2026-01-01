from include.config.variables import S3_ACCESS 
import boto3

def s3_resource(): 
    s3 = boto3.resource(
        's3',
        endpoint_url = S3_ACCESS['s3_endpoint'], 
        aws_access_key_id = S3_ACCESS['aws_access_key'],
        aws_secret_access_key = S3_ACCESS['aws_secret_key'],
    )
    return s3

def s3_connection(): 
    s3 = boto3.client(
        's3',
        endpoint_url = S3_ACCESS['s3_endpoint'], 
        aws_access_key_id = S3_ACCESS['aws_access_key'],
        aws_secret_access_key = S3_ACCESS['aws_secret_key']
    )
    return s3
