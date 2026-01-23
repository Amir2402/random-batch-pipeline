from airflow.sdk import BaseOperator
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class SlackNotifier(BaseOperator):
    def __init__(self, slack_token, channel_id, bot_name, slack_message, now_timestamp, **kwargs):
        super().__init__(**kwargs)

        self.channel_id = channel_id
        self.bot_name = bot_name
        self.message = slack_message
        self.slack_token = slack_token
        self.now_timestamp = now_timestamp

        self.client = WebClient(token = slack_token)
    
    def execute(self, context):
        try:
            response = self.client.chat_postMessage(channel = self.channel_id, text = self.message,
                                                    username = self.bot_name)
            self.log.info(f"Message sent successfully! - {self.now_timestamp}")

        except SlackApiError as e:
            self.log.info(f"Error occured when alerting slack channel! - {self.now_timestamp}")
            raise