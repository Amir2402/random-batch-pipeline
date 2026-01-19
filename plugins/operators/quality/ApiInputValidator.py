from airflow.sdk import BaseOperator
from pydantic import ValidationError
from include.utils.S3HelperFunctions import S3HelperFunctions
from include.utils.models import Root
from include.utils.SlackNotifier import notify_slack

class ApiInputValidator(BaseOperator):
    def __init__(self, bucket_name, now_timestamp, file_name, slack_api_key, channel_id, bot_name, message, **kwargs): 
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.s3Helper = S3HelperFunctions(now_timestamp)

        self.slack_api_key = slack_api_key
        self.channel_id = channel_id
        self.bot_name = bot_name
        self.message = message
    
    def execute(self, context):
        data_to_validate = self.s3Helper.read_json_s3(self.file_name, self.bucket_name)
        
        try:
            parsed = Root(**data_to_validate)
            self.log.info('data validation passed successfully!')

        except:
            self.log.info('alert API schema change!')
            notify_slack(self.slack_api_key, self.channel_id, self.bot_name, self.message)
