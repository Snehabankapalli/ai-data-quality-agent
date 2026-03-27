"""
Slack notifier for pipeline failure and data quality alerts.
Formats diagnosis results into rich Slack Block Kit messages.
"""

import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

SEVERITY_EMOJI = {
    "LOW":      "ℹ️",
    "MEDIUM":   "⚠️",
    "HIGH":     "🔴",
    "CRITICAL": "🚨",
}

SEVERITY_COLOR = {
    "LOW":      "#36a64f",
    "MEDIUM":   "#ff9800",
    "HIGH":     "#e53935",
    "CRITICAL": "#b71c1c",
}


class SlackNotifier:
    def __init__(self, webhook_url: str):
        self._webhook_url = webhook_url

    def send_pipeline_alert(self, context, diagnosis) -> bool:
        """Send a pipeline failure diagnosis alert to Slack."""
        emoji = SEVERITY_EMOJI.get(diagnosis.severity, "⚠️")
        color = SEVERITY_COLOR.get(diagnosis.severity, "#ff9800")

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} PIPELINE FAILURE — {diagnosis.severity}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Pipeline:*\n{context.pipeline_name}"},
                    {"type": "mrkdwn", "text": f"*Failed Task:*\n{context.task_name}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"},
                    {"type": "mrkdwn", "text": f"*Confidence:*\n{diagnosis.confidence}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*ROOT CAUSE*\n{diagnosis.root_cause}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*RECOMMENDED FIX*\n{diagnosis.fix_description}",
                },
            },
        ]

        if diagnosis.sql_fix:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*SQL TO VERIFY*\n```{diagnosis.sql_fix}```",
                },
            })

        auto_heal_text = (
            f"YES — {diagnosis.auto_heal_action}" if diagnosis.auto_healable
            else "NO (manual action required)"
        )
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Alert Score: *{diagnosis.alert_score}/100*  |  "
                        f"Auto-heal: *{auto_heal_text}*"
                    ),
                }
            ],
        })

        return self._post({"attachments": [{"color": color, "blocks": blocks}]})

    def send_data_quality_alert(self, context, diagnosis) -> bool:
        """Send a data quality anomaly alert to Slack."""
        emoji = SEVERITY_EMOJI.get(diagnosis.severity, "⚠️")

        payload = {
            "text": (
                f"{emoji} *DATA QUALITY ALERT — {diagnosis.severity}*\n"
                f"*Table:* `{context.table_name}`  *Column:* `{context.column_name}`\n"
                f"*Issue:* {context.issue_type.replace('_', ' ').title()} "
                f"({context.current_value:.1f}% vs baseline {context.baseline_value:.1f}%)\n\n"
                f"*Root Cause:* {diagnosis.root_cause}\n\n"
                f"*Fix:*\n{diagnosis.fix_description}\n"
            )
        }

        if diagnosis.sql_fix:
            payload["text"] += f"\n*SQL Fix:*\n```{diagnosis.sql_fix}```"

        return self._post(payload)

    def _post(self, payload: dict) -> bool:
        try:
            resp = requests.post(self._webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            return True
        except requests.RequestException as exc:
            logger.error("Slack notification failed: %s", exc)
            return False
