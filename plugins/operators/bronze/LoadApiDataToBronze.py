from airflow.sdk import BaseOperator
import requests as rq
from datetime import datetime
from include.utils.S3HelperFunctions import s3_resource, write_to_s3
import random
import json
import uuid

class LoadUserDataToBronze(BaseOperator): 
    def __init__(self, bucket_name, api_url, , **kwargs): 
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.api_url = api_url
    
    def execute(self, context):
        # if self.bucket_name not in BUCKETS.values():
        #     self.log.info('Creating a bucket for a non valid layer!') 
        
        # else: 
        #     s3 = s3_resource()
        #     try:
        #         bucket = s3.Bucket(self.bucket_name)
        #         if bucket.creation_date:
        #             self.log.info(f'bucket created on {bucket.creation_date} exists!')

        #         else: 
        #             s3.create_bucket(Bucket = self.bucket_name)
        #             self.log.info(f"bucket {self.bucket_name} created successfully!")

        #     except Exception as e: 
        #         self.log.info('an error occured: ', e)