from airflow.sdk import BaseOperator
import requests as rq
from datetime import datetime
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.config.variables import BUCKETS
import random
import json
import uuid

class LoadUserDataToBronze(BaseOperator): 
    def __init__(self, bucket_name, api_url, now_timestamp, **kwargs): 
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.api_url = api_url
        self.now_timestamp = now_timestamp
        self.s3Helper = S3HelperFunctions(self.now_timestamp)
    
    def execute(self, context):
        user_data = self.fetch_api_data()
            
        self.log.info('Writing user_data to bronze layer')
        self.s3Helper.write_to_s3(user_data, 'user_data', self.bucket_name)
        self.log.info('Object written successfully!')

        return user_data

    def fetch_api_data(self):
        user_data = []
        self.log.info('Fetching API user data')
        
        for itr in range(1, 100):
            req = rq.get(self.api_url)

            try:
                user_record = json.loads(req.content.decode())

                user_id = str(uuid.uuid4())
                quantity_bought = random.randint(1, 10)
                unit_price = random.uniform(1, 100)

                user_record['id'] = user_id
                user_record['quantity'] = quantity_bought
                user_record['unit_price'] = unit_price
                user_record['row_no'] = itr
                
                user_data.append(user_record)

            except:
                self.log.info(f'Failed to fetch API data, item {itr}')

        return user_data