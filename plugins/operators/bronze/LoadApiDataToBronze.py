from airflow.sdk import BaseOperator
from plugins.operators.BaseOperatorWithLineage import BaseOperatorWithLineage
import requests as rq
from datetime import datetime
from include.utils.data import PRODUCTS
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.NotifySlack import notify_slack
import random
import json
from airflow.providers.openlineage.extractors.base import OperatorLineage
from openlineage.client.generated.base import Dataset

class LoadUserDataToBronze(BaseOperatorWithLineage): 
    def __init__(self, bucket_name, api_url, api_key, now_timestamp, file_name, lineage_output, **kwargs): 
        super().__init__(
            lineage_output = lineage_output,
            **kwargs
        )
        self.bucket_name = bucket_name
        self.api_url = api_url
        self.api_key = api_key
        self.now_timestamp = now_timestamp
        self.file_name = file_name
        self.s3Helper = S3HelperFunctions(self.now_timestamp)

    def execute(self, context):
        try: 
            sales_data = self.fetch_api_data()
            self.log.info('Writing sales_data to bronze layer') 
            self.s3Helper.write_to_s3(sales_data, self.file_name, self.bucket_name)
            self.log.info('Object written successfully!')
        except: 
            self.log.info('Failed to ingest API user data!')
            notify_slack(
                self.now_timestamp,
                f'Failed to ingest data on {self.now_timestamp}!'
            )
            self.log.info('Alerting ingestion failure on Slack!')
            raise

    def fetch_api_data(self):
        self.log.info('Fetching API user data')

        try:
            headers = {"X-Api-Key": self.api_key}
            req = rq.get(self.api_url, headers=headers)
            sales_data = json.loads(req.content.decode())
            product = random.choice(PRODUCTS)
            quantity_bought = random.randint(1, 10)

            for sales_record in sales_data:
                sales_record['quantity'] = quantity_bought
                sales_record['product_id'] = product['product_id']
                sales_record['product_name'] = product['product_name']
                sales_record['unit_price'] = product['unit_price']
                sales_record['event_ts'] = str(datetime.now())
        
        except:
            self.log.info(f'Failed to fetch API data')
            raise
    
        return sales_data