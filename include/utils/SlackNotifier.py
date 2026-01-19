from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def notify_slack(slack_token, channel_id, bot_name, slack_message):
    client = WebClient(token = slack_token)

    channel_id = channel_id
    bot_name = bot_name
    message = slack_message

    try:
        response = client.chat_postMessage(channel = channel_id, text = message,username = bot_name)
        print("Message sent successfully!")

    except SlackApiError as e:
        print(f"Error sending message: {e}")