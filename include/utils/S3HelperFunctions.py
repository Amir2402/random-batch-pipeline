from include.config.variables import S3_ACCESS 
import boto3 
import json 

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

def write_to_s3(object_to_store, base_filename, current_timestamp, bucket_name):
    s3 = s3_connection()
    
    current_year = current_timestamp.year 
    current_month = current_timestamp.month
    current_day = current_timestamp.day
    current_hour = current_timestamp.hour 
    
    object_key = f"{base_filename}/year={current_year}/month={current_month}/day={current_day}/hour={current_hour}.json"
    object_body = {"data": object_to_store}

    s3.put_object(
        Bucket = bucket_name,
        Key = object_key,
        Body = json.dumps(object_body).encode("utf-8"),
        ContentType = "application/json"
    )