from airflow.sdk import BaseOperator
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from include.config.variables import CHANNEL_ID, BOT_NAME, SLACK_API_KEY
import logging

def notify_slack(now, message):
    channel_id = CHANNEL_ID
    bot_name = BOT_NAME
    slack_token = SLACK_API_KEY
    now_timestamp = now

    client = WebClient(token = slack_token)

    try:
        response = client.chat_postMessage(channel = channel_id, text = message,
                                                username = bot_name)
        logging.info(f"Message sent successfully! - {now_timestamp}")

    except SlackApiError as e:
        logging.error(f"Error occured when alerting slack channel! - {now_timestamp}")
        raise