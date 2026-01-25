from airflow.sdk import BaseOperator
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class SlackNotifier(BaseOperator):
    def __init__(self, slack_token, channel_id, bot_name, now_timestamp, **kwargs):
        super().__init__(**kwargs)

        self.channel_id = channel_id
        self.bot_name = bot_name
        self.slack_token = slack_token
        self.now_timestamp = now_timestamp

        self.client = WebClient(token = slack_token)
    
    def execute(self, context):
        try:
            messages = context['ti'].xcom_pull(task_ids = ['validate_user_data_schema', 
                                                           'load_API_data_to_bronze'], key = 'alert_message')
            message = [message for message in messages if message != None][0]
            
            response = self.client.chat_postMessage(channel = self.channel_id, text = message,
                                                    username = self.bot_name)
            self.log.info(f"Message sent successfully! - {self.now_timestamp}")

        except SlackApiError as e:
            self.log.info(f"Error occured when alerting slack channel! - {self.now_timestamp}")
            raise