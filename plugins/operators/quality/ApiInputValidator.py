from airflow.sdk import BaseOperator
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.models import Root

class ApiInputValidator(BaseOperator):
    def __init__(self, bucket_name, now_timestamp, file_name, **kwargs): 
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.s3Helper = S3HelperFunctions(now_timestamp)
    
    def execute(self, context):
        data_to_validate = self.s3Helper.read_json_s3(self.file_name, self.bucket_name)
        
        try:
            parsed = Root(**data_to_validate)
            self.log.info('Data validation passed successfully!')

        except:
            self.log.info('Alert API schema change!')
            raise
