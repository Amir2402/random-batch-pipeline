from airflow.sdk import BaseOperator
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.queries import connect_duck_db_to_S3
from include.utils.NotifySlack import notify_slack

class LoadTableToGold(BaseOperator):
    def __init__(self, delta_table_name, duckdb_table_name, input_bucket_name, output_bucket_name, now_timestamp, query, **kwargs):
        super().__init__(**kwargs) 
        self.delta_table_name = delta_table_name
        self.duckdb_table_name = duckdb_table_name
        self.query = query
        self.now_timestamp = now_timestamp

        self.S3Helper = S3HelperFunctions(self.now_timestamp)
        self.conn = connect_duck_db_to_S3()

        self.input_bucket = input_bucket_name
        self.output_bucket = output_bucket_name

    def execute(self, context):
        try:
            self.log.info(f'Reading {self.delta_table_name} data from delta tables!')
            self.S3Helper.read_delta_from_s3(self.delta_table_name, self.duckdb_table_name,
                                            self.conn, self.input_bucket)

            self.log.info(f'transforming {self.duckdb_table_name} table!')
            self.conn.sql(self.query)

            self.log.info(f'Loading gold_{self.duckdb_table_name} table into gold layer!')
            self.S3Helper.write_delta_to_s3(f"gold_{self.duckdb_table_name}", self.conn, self.output_bucket)
        
        except:
            message = f"Error occured while processing gold_{self.duckdb_table_name}!"
            self.log.error(message)
            notify_slack(
                self.now_timestamp,
                f"{message} - {self.now_timestamp}!"
            )   