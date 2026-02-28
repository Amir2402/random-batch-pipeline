from airflow.sdk import BaseOperator
from include.utils.queries import connect_duck_db_to_S3
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.NotifySlack import notify_slack
import great_expectations as ge

class QualityChecks(BaseOperator):
    def __init__(self, delta_table_name, duckdb_table_name, input_bucket_name, now_timestamp, **kwargs):
        super().__init__(**kwargs)
        self.delta_table_name = delta_table_name
        self.duckdb_table_name = duckdb_table_name
        self.input_bucket_name = input_bucket_name
        self.now_timestamp = now_timestamp

        self.conn = connect_duck_db_to_S3()  
        self.S3Helper = S3HelperFunctions(self.now_timestamp) 
    
    def execute(self, context):
        try:
            self.log.info("Reading silver data to perform quality checks")
            self.S3Helper.read_delta_from_s3(self.delta_table_name, self.duckdb_table_name,
                                            self.conn, self.input_bucket_name)
            df = self.conn.sql('SELECT * FROM silver_data;').fetchdf()
            
            self.log.info("Performing checks!")
            results = self.run_quality_checks(df)

            for res in results['results']:
                if not res['success']:
                    expectation_config = res["expectation_config"]
                    message = f"Check \"{expectation_config["expectation_type"]}\" on {expectation_config["kwargs"]["column"]} failed"
                    self.log.info(message)
                    notify_slack(
                    self.now_timestamp,
                    f"{message} - {self.now_timestamp}!"
                    )

        except:
            message = f"Error occured while performing data quality checks {self.now_timestamp}"
            self.log.error(message)
            notify_slack(
                self.now_timestamp,
                f"{message} - {self.now_timestamp}!"
            )
            raise
        
    def run_quality_checks(self, df):
        ge_df = ge.from_pandas(df)
        ge_df.expect_column_values_to_not_be_null("user_id")
        ge_df.expect_column_values_to_not_be_null("product_id")
        ge_df.expect_column_values_to_not_be_null("date_id")
        ge_df.expect_column_values_to_not_be_null("event_ts")

        ge_df.expect_column_values_to_be_between("unit_price", min_value=0)
        results = ge_df.validate() 

        return results