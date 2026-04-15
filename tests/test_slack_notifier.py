"""Tests for SlackNotifier — mocks requests.post."""

import pytest
from unittest.mock import patch, MagicMock

from src.tools.slack_notifier import SlackNotifier, SEVERITY_EMOJI, SEVERITY_COLOR


WEBHOOK = "https://hooks.slack.com/services/test/webhook"


class TestSlackNotifierPipelineAlert:
    def setup_method(self):
        self.notifier = SlackNotifier(webhook_url=WEBHOOK)

    def test_sends_post_request(self, pipeline_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            result = self.notifier.send_pipeline_alert(pipeline_context, diagnosis_high)

        assert result is True
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == WEBHOOK

    def test_payload_contains_pipeline_name(self, pipeline_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            self.notifier.send_pipeline_alert(pipeline_context, diagnosis_high)

        payload = mock_post.call_args[1]["json"]
        payload_str = str(payload)
        assert pipeline_context.pipeline_name in payload_str

    def test_payload_contains_sql_fix_when_present(self, pipeline_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            self.notifier.send_pipeline_alert(pipeline_context, diagnosis_high)

        payload_str = str(mock_post.call_args[1]["json"])
        assert diagnosis_high.sql_fix in payload_str

    def test_returns_false_on_request_error(self, pipeline_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.side_effect = Exception("connection refused")
            result = self.notifier.send_pipeline_alert(pipeline_context, diagnosis_high)

        assert result is False

    def test_auto_heal_text_when_not_healable(self, pipeline_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            self.notifier.send_pipeline_alert(pipeline_context, diagnosis_high)

        payload_str = str(mock_post.call_args[1]["json"])
        assert "manual action required" in payload_str


class TestSlackNotifierDataQualityAlert:
    def setup_method(self):
        self.notifier = SlackNotifier(webhook_url=WEBHOOK)

    def test_sends_post_request(self, dq_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            result = self.notifier.send_data_quality_alert(dq_context, diagnosis_high)

        assert result is True

    def test_payload_contains_table_name(self, dq_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            self.notifier.send_data_quality_alert(dq_context, diagnosis_high)

        payload_str = str(mock_post.call_args[1]["json"])
        assert dq_context.table_name in payload_str

    def test_payload_contains_sql_fix(self, dq_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            mock_post.return_value.raise_for_status = MagicMock()
            self.notifier.send_data_quality_alert(dq_context, diagnosis_high)

        payload_str = str(mock_post.call_args[1]["json"])
        assert diagnosis_high.sql_fix in payload_str

    def test_returns_false_on_http_error(self, dq_context, diagnosis_high):
        with patch("requests.post") as mock_post:
            import requests
            mock_post.return_value = MagicMock()
            mock_post.return_value.raise_for_status.side_effect = requests.RequestException("500")
            result = self.notifier.send_data_quality_alert(dq_context, diagnosis_high)

        assert result is False


class TestSeverityConstants:
    def test_all_severities_have_emoji(self):
        for level in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
            assert level in SEVERITY_EMOJI
            assert SEVERITY_EMOJI[level]

    def test_all_severities_have_color(self):
        for level in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
            assert level in SEVERITY_COLOR
            assert SEVERITY_COLOR[level].startswith("#")

    def test_critical_color_is_darkest_red(self):
        # CRITICAL should be darker than HIGH
        assert SEVERITY_COLOR["CRITICAL"] != SEVERITY_COLOR["HIGH"]
