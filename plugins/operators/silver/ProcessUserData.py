from airflow.sdk import BaseOperator
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.queries import connect_duck_db_to_S3, read_json_from_bronze, sales_transform_silver
from include.utils.S3HelperFunctions import S3HelperFunctions

class ProcessUserData(BaseOperator):
    def __init__(self, bucket_name, table_name, now_timestamp, **kwargs): 
        super().__init__(**kwargs)

        self.S3Helper = S3HelperFunctions(now_timestamp)

        self.conn = connect_duck_db_to_S3()
        self.bucket_name = bucket_name 
        self.table_name = table_name
        
        self.now_timestamp = now_timestamp
        self.day = self.now_timestamp.day 
        self.month = self.now_timestamp.month
        self.year = self.now_timestamp.year

    def execute(self, context):
        self.log.info('Reading user data from bronze layer!')
        self.conn.sql(read_json_from_bronze(self.table_name, self.year, self.month, self.day))
        
        self.log.info('Transforming user data!')
        self.conn.sql(sales_transform_silver)

        self.log.info('Writing data to silver layer in Delta format!')
        self.S3Helper.write_delta_to_s3(f"silver_{self.table_name}", self.conn, self.bucket_name)

