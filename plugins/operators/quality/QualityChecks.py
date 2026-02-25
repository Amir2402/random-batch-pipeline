from airflow.sdk import BaseOperator
from include.utils.queries import connect_duck_db_to_S3
from include.utils.S3HelperFunctions import S3HelperFunctions
import great_expectations as ge

class QualityChecks(BaseOperator):
    def __init__(self, delta_table_name, duckdb_table_name, input_bucket_name, now_timestamp, **kwargs):
        super().__init__(**kwargs)
        self.delta_table_name = delta_table_name
        self.duckdb_table_name = duckdb_table_name
        self.input_bucket_name = input_bucket_name

        self.conn = connect_duck_db_to_S3()  
        self.S3Helper = S3HelperFunctions(now_timestamp) 
    
    def execute(self, context):
        self.log.info("Reading silver data to perform quality checks")
        self.S3Helper.read_delta_from_s3(self.delta_table_name, self.duckdb_table_name,
                                         self.conn, self.input_bucket_name)
        df = self.conn.sql('SELECT * FROM silver_data;').fetchdf()
        
        self.log.info("Performing checks!")
        results = self.run_quality_checks(df)

        self.log.info(results)
        
    
    def run_quality_checks(self, df):
        ge_df = ge.from_pandas(df)
        ge_df.expect_column_values_to_not_be_null("user_id")
        ge_df.expect_column_values_to_not_be_null("product_id")
        ge_df.expect_column_values_to_not_be_null("date_id")
        ge_df.expect_column_values_to_not_be_null("event_ts")

        ge_df.expect_column_values_to_be_between("unit_price", min_value=0)
        results = ge_df.validate() 

        return results