from airflow.sdk import BaseOperator
from pydantic import ValidationError
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.models import sales_data
from include.utils.NotifySlack import notify_slack

class ApiInputValidator(BaseOperator):
    def __init__(self, bucket_name, now_timestamp, file_name, **kwargs): 
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.now_timestamp = now_timestamp

        self.s3Helper = S3HelperFunctions(now_timestamp)
    
    def execute(self, context):
        data_to_validate = self.s3Helper.read_json_s3(self.file_name, self.bucket_name)
        
        try:
            parsed = sales_data(**data_to_validate)
            self.log.info('Data validation passed successfully!')

        except ValidationError:
            self.log.info(f'Alerting slack on api schema change')
            notify_slack(
                self.now_timestamp,
                f'Api schema change - {self.now_timestamp}'
            )
        
        except:
            message = "Error occured while validating API schema!"
            self.log.error(f'{message}')
            notify_slack(
                self.now_timestamp,
                f'{message} - {self.now_timestamp}'
            )
            raise