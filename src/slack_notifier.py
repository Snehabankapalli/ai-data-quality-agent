"""
Slack notifier for data quality alerts.
Sends rich Block Kit messages with severity-coded formatting.
"""

import os
from enum import Enum

import structlog
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = structlog.get_logger()


class Severity(str, Enum):
    OK = "ok"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


SEVERITY_EMOJI = {
    Severity.OK: "✅",
    Severity.LOW: "🟡",
    Severity.MEDIUM: "🟠",
    Severity.HIGH: "🔴",
    Severity.CRITICAL: "🚨",
}

SEVERITY_COLOR = {
    Severity.OK: "#36a64f",
    Severity.LOW: "#ECB22E",
    Severity.MEDIUM: "#E37C3B",
    Severity.HIGH: "#E01E5A",
    Severity.CRITICAL: "#8B0000",
}


class SlackNotifier:
    """Sends formatted data quality alerts to Slack channels."""

    def __init__(self):
        self._client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
        self._alert_channel = os.environ.get("SLACK_ALERT_CHANNEL", "#data-quality-alerts")
        self._escalation_channel = os.environ.get("SLACK_ESCALATION_CHANNEL", "#data-oncall")

    def send_alert(
        self,
        title: str,
        severity: str,
        check_summary: str,
        claude_analysis: str,
        recommended_action: str,
        healing_triggered: bool = False,
        healing_result: str | None = None,
    ) -> bool:
        """
        Send a formatted data quality alert to Slack.

        Args:
            title:               Short alert title
            severity:            one of ok/low/medium/high/critical
            check_summary:       Raw check output summary
            claude_analysis:     Claude's root cause analysis
            recommended_action:  Claude's recommended next step
            healing_triggered:   Whether auto-healing was attempted
            healing_result:      Result of healing action if triggered

        Returns:
            True if message sent successfully
        """
        sev = Severity(severity.lower()) if severity.lower() in Severity._value2member_map_ else Severity.MEDIUM
        emoji = SEVERITY_EMOJI[sev]
        color = SEVERITY_COLOR[sev]

        channel = (
            self._escalation_channel
            if sev in (Severity.CRITICAL, Severity.HIGH)
            else self._alert_channel
        )

        healing_section = []
        if healing_triggered:
            status = "✅ Auto-heal succeeded" if "success" in (healing_result or "").lower() else "❌ Auto-heal failed"
            healing_section = [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Auto-Healing:* {status}\n>{healing_result}"},
                },
            ]

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} {title}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Severity:*\n{sev.value.upper()}"},
                    {"type": "mrkdwn", "text": f"*Channel:*\n{channel}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Check Details:*\n```{check_summary}```"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*🤖 Claude's Analysis:*\n{claude_analysis}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Recommended Action:*\n> {recommended_action}"},
            },
            *healing_section,
        ]

        try:
            self._client.chat_postMessage(
                channel=channel,
                text=f"{emoji} Data Quality Alert: {title}",
                attachments=[{"color": color, "blocks": blocks}],
            )
            logger.info("slack_alert_sent", channel=channel, severity=sev.value, title=title)
            return True
        except SlackApiError as e:
            logger.error("slack_alert_failed", error=str(e), channel=channel)
            return False

    def send_resolution(self, title: str, resolution_message: str) -> bool:
        """Send a resolution notification when an issue is resolved."""
        try:
            self._client.chat_postMessage(
                channel=self._alert_channel,
                text=f"✅ Resolved: {title}",
                blocks=[
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"✅ *Resolved:* {title}\n{resolution_message}",
                        },
                    }
                ],
            )
            return True
        except SlackApiError as e:
            logger.error("slack_resolution_failed", error=str(e))
            return False
