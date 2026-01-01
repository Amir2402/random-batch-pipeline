from airflow.sdk import BaseOperator
from include.config.variables import BUCKETS
from include.utils.S3HelperFunctions import s3_resource

class CreateBucketOperator(BaseOperator): 
    def __init__(self, bucket_name, **kwargs): 
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
    
    def execute(self, context):
        if self.bucket_name not in BUCKETS.values():
            self.log.info('Creating a bucket for a non valid layer!') 
        
        else: 
            s3 = s3_resource()
            try:
                bucket = s3.Bucket(self.bucket_name)
                if bucket.creation_date:
                    self.log.info(f'bucket created on {bucket.creation_date} exists!')

                else: 
                    s3.create_bucket(Bucket = self.bucket_name)
                    self.log.info(f"bucket {self.bucket_name} created successfully!")

            except Exception as e: 
                self.log.info('an error occured: ', e)
