from slack_sdk import WebClient

from ..utils import StringTemplates
from .core import Destination

supported_actions = ("template-message", "message")


class SlackDestination(Destination):
    def validate(self):
        slack_bot_token = self.connector_config.get("creds", {}).get("slackBotToken")
        if not slack_bot_token:
            raise ValueError("Slack bot token missing in creds")

        destination_params: dict = self.target_config.get("destinationParams", {})
        action = destination_params.get("action")
        if action not in supported_actions:
            raise ValueError(
                f"Action not in supported action. Action {action}. \
                             Supported Actions {supported_actions}"
            )

        if "channel" not in destination_params:
            raise ValueError("channel not found in destination params")

        if action == "template-message" and "template" not in destination_params:
            raise ValueError("template required for templatised message to work")

    def get_template_message(self):
        string_template = StringTemplates(
            self.target_config["destinationParams"]["template"], self.input
        )
        return string_template.render_template()

    def send(self):
        self.validate()
        client = WebClient(token=self.connector_config["creds"]["slackBotToken"])
        channel = self.target_config["destinationParams"]["channel"]

        action = self.target_config["destinationParams"]["action"]
        message = ""
        if action == "message":
            message = StringTemplates.stringify(self.input)
        elif action == "template-message":
            message = self.get_template_message()

        client.chat_postMessage(channel=channel, text=message)
        {"success": "True", "message_sent": message}
