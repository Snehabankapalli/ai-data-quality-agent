"""Tests for main entrypoint — config loading and orchestration."""

import os
import pytest
from unittest.mock import patch, MagicMock


class TestLoadConfig:
    def test_loads_all_required_env_vars(self):
        from src.agent.main import load_config

        env = {
            "ANTHROPIC_API_KEY": "test-key",
            "SNOWFLAKE_ACCOUNT": "acct123",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
        }
        with patch.dict(os.environ, env, clear=False):
            config = load_config()

        assert config["anthropic_api_key"] == "test-key"
        assert config["snowflake"]["account"] == "acct123"
        assert config["snowflake"]["user"] == "user"
        assert config["snowflake"]["password"] == "pass"
        assert config["slack_webhook"] == "https://hooks.slack.com/test"

    def test_raises_on_missing_required_var(self):
        from src.agent.main import load_config

        env = {
            "ANTHROPIC_API_KEY": "key",
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "user",
            # SNOWFLAKE_PASSWORD missing
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(EnvironmentError) as exc_info:
                load_config()
        assert "SNOWFLAKE_PASSWORD" in str(exc_info.value)

    def test_snowflake_defaults_applied(self):
        from src.agent.main import load_config

        env = {
            "ANTHROPIC_API_KEY": "key",
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_config()

        assert config["snowflake"]["database"] == "FINTECH_DB"
        assert config["snowflake"]["schema"] == "STAGING"

    def test_snowflake_database_override(self):
        from src.agent.main import load_config

        env = {
            "ANTHROPIC_API_KEY": "key",
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "SNOWFLAKE_DATABASE": "CUSTOM_DB",
            "SNOWFLAKE_SCHEMA": "CUSTOM_SCHEMA",
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_config()

        assert config["snowflake"]["database"] == "CUSTOM_DB"
        assert config["snowflake"]["schema"] == "CUSTOM_SCHEMA"


class TestRunDataQualityChecks:
    def test_sends_alert_when_score_above_threshold(self):
        from src.agent.main import run_data_quality_checks, ALERT_SCORE_THRESHOLD
        from src.monitors.snowflake_monitor import QualityAnomaly
        from src.agent.diagnosis_engine import Diagnosis, Severity, Confidence

        monitor = MagicMock()
        engine = MagicMock()
        notifier = MagicMock()

        anomaly = QualityAnomaly(
            table="STAGING.STG_CARD_TRANSACTIONS",
            column="merchant_id",
            issue_type="null_spike",
            current_value=40.0,
            baseline_value=1.0,
            deviation_pct=39.0,
            sample_rows=[],
        )
        monitor.check_table.return_value = [anomaly]
        engine.diagnose_data_quality_issue.return_value = Diagnosis(
            root_cause="vendor change",
            severity=Severity.HIGH,
            confidence=Confidence.HIGH,
            fix_description="Fix vendor API.",
            sql_fix=None,
            auto_healable=False,
            auto_heal_action=None,
            alert_score=ALERT_SCORE_THRESHOLD + 1,
        )

        run_data_quality_checks(monitor, engine, notifier)

        notifier.send_data_quality_alert.assert_called_once()

    def test_suppresses_alert_when_score_below_threshold(self):
        from src.agent.main import run_data_quality_checks, ALERT_SCORE_THRESHOLD
        from src.monitors.snowflake_monitor import QualityAnomaly
        from src.agent.diagnosis_engine import Diagnosis, Severity, Confidence

        monitor = MagicMock()
        engine = MagicMock()
        notifier = MagicMock()

        anomaly = QualityAnomaly(
            table="STAGING.STG_CARD_TRANSACTIONS",
            column="amount_cents",
            issue_type="null_spike",
            current_value=2.0,
            baseline_value=1.5,
            deviation_pct=0.5,
            sample_rows=[],
        )
        monitor.check_table.return_value = [anomaly]
        engine.diagnose_data_quality_issue.return_value = Diagnosis(
            root_cause="minor blip",
            severity=Severity.LOW,
            confidence=Confidence.LOW,
            fix_description="Monitor.",
            sql_fix=None,
            auto_healable=False,
            auto_heal_action=None,
            alert_score=ALERT_SCORE_THRESHOLD - 1,
        )

        run_data_quality_checks(monitor, engine, notifier)

        notifier.send_data_quality_alert.assert_not_called()

    def test_continues_on_monitor_exception(self):
        from src.agent.main import run_data_quality_checks

        monitor = MagicMock()
        monitor.check_table.side_effect = Exception("snowflake down")
        engine = MagicMock()
        notifier = MagicMock()

        # Should not raise
        run_data_quality_checks(monitor, engine, notifier)
        notifier.send_data_quality_alert.assert_not_called()
