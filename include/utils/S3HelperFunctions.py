from include.config.variables import S3_ACCESS 
from deltalake.writer import write_deltalake
import boto3 
import json

class S3HelperFunctions():
    def __init__(self, current_timestamp):
        self.s3_endpoint = S3_ACCESS['s3_endpoint']
        self.aws_access_key = S3_ACCESS['aws_access_key']
        self.aws_secret_key = S3_ACCESS['aws_secret_key']

        self.current_year = current_timestamp.year 
        self.current_month = current_timestamp.month
        self.current_day = current_timestamp.day
        self.current_hour = current_timestamp.hour 
        
    def s3_resource(self): 
        s3 = boto3.resource(
            's3',
            endpoint_url = self.s3_endpoint, 
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_key,
        )
        return s3

    def s3_client(self): 
        s3 = boto3.client(
            's3',
            endpoint_url = self.s3_endpoint, 
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_key
        )
        return s3

    def write_to_s3(self, object_to_store, base_filename, bucket_name):
        s3 = self.s3_client()
        
        object_key = f"{base_filename}/year={self.current_year}/month={self.current_month}/day={self.current_day}/hour={self.current_hour}.json"
        object_body = {"data": object_to_store}

        try:
            s3.put_object(
                Bucket = bucket_name,
                Key = object_key,
                Body = json.dumps(object_body).encode("utf-8"),
                ContentType = "application/json"
            )
        
        except:
            print(f'Error occured when writing object {object_key}')
    
    def read_json_s3(self, base_filename, bucket_name): 
        s3 = self.s3_resource()

        object_key = f"{base_filename}/year={self.current_year}/month={self.current_month}/day={self.current_day}/hour={self.current_hour}.json"
        object_to_read = s3.Object(bucket_name, object_key)
    
        file_content = object_to_read.get()['Body'].read().decode('utf-8')

        return json.loads(file_content)

    def write_delta_to_s3(self, table_name, conn, layer): # not tested yet
        storage_options = {
            "AWS_ACCESS_KEY_ID": S3_ACCESS['aws_access_key'], 
            "AWS_SECRET_ACCESS_KEY": S3_ACCESS['aws_secret_key'],
            "AWS_ENDPOINT_URL": S3_ACCESS['s3_endpoint']
        }
        
        df = conn.sql(f"SELECT * FROM {table_name};").arrow()
        s3_path = f"s3://{layer}/{table_name}"

        write_deltalake(
            s3_path,
            data = df, 
            partition_by = ["year", "month", "day"],
            storage_options = storage_options,
            mode = 'append'
        )