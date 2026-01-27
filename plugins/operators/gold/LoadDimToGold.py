from airflow.sdk import BaseOperator
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.queries import connect_duck_db_to_S3, select_user_dimension

class LoadDimToGold(BaseOperator):
    def __init__(self, delta_table_name, duckdb_table_name, input_bucket_name, output_bucket_name, now_timestamp, query, **kwargs):
        super().__init__(**kwargs) 
        self.delta_table_name = delta_table_name
        self.duckdb_table_name = duckdb_table_name
        self.query = query

        self.S3Helper = S3HelperFunctions(now_timestamp)
        self.conn = connect_duck_db_to_S3()

        self.input_bucket = input_bucket_name
        self.output_bucket = output_bucket_name

    def execute(self, context):
        self.log.info(f'Reading {self.delta_table_name} data from delta tables!')
        self.S3Helper.read_delta_from_s3(self.delta_table_name, self.duckdb_table_name,
                                         self.conn, self.input_bucket)
        
        self.log.info(f'transforming {self.duckdb_table_name} dimension!')
        self.conn.sql(self.query)

        self.log.info(f'Loading {self.duckdb_table_name} dimension into gold layer!')
        self.S3Helper.write_delta_to_s3(self.duckdb_table_name, self.conn, self.output_bucket)