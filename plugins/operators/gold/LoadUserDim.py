from airflow.sdk import BaseOperator
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.queries import connect_duck_db_to_S3, select_user_dimension

class LoadUserDimension(BaseOperator):
    def __init__(self, delta_table_name, duckdb_table_name, input_bucket_name, output_bucket_name, now_timestamp, **kwargs):
        super().__init__(**kwargs) 
        self.delta_table_name = delta_table_name
        self.duckdb_table_name = duckdb_table_name

        self.S3Helper = S3HelperFunctions(now_timestamp)
        self.conn = connect_duck_db_to_S3()

        self.input_bucket = input_bucket_name
        self.output_bucket = output_bucket_name

    def execute(self, context):
        self.log.info('Reading User data from delta tables!')
        self.S3Helper.read_delta_from_s3(self.delta_table_name, self.duckdb_table_name,
                                         self.conn, self.input_bucket)
        
        self.log.info('transforming User dimension!')
        self.conn.sql(select_user_dimension)

        self.log.info('Loading User dimension into gold layer!')
        self.S3Helper.write_delta_to_s3(self.duckdb_table_name, self.conn, self.output_bucket)