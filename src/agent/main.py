"""
AI Data Pipeline Agent — main entrypoint.
Polls Airflow, Glue, Snowflake, and dbt for failures and quality issues.
Diagnoses using Claude API and routes to Slack or auto-heal.
"""

import logging
import os
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from src.agent.diagnosis_engine import DiagnosisEngine, PipelineContext, DataQualityContext
from src.monitors.snowflake_monitor import SnowflakeMonitor
from src.tools.slack_notifier import SlackNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 60
ALERT_SCORE_THRESHOLD = 60   # Only alert if score > this
TABLES_TO_MONITOR = [
    ("STAGING.STG_CARD_TRANSACTIONS", ["merchant_id", "account_token", "amount_cents"]),
    ("STAGING.STG_CARD_AUTHORIZATIONS", ["authorization_code", "response_code"]),
]


def load_config() -> dict:
    required = [
        "ANTHROPIC_API_KEY",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SLACK_WEBHOOK_URL",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")

    return {
        "anthropic_api_key": os.environ["ANTHROPIC_API_KEY"],
        "snowflake": {
            "account":  os.environ["SNOWFLAKE_ACCOUNT"],
            "user":     os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "database": os.environ.get("SNOWFLAKE_DATABASE", "FINTECH_DB"),
            "schema":   os.environ.get("SNOWFLAKE_SCHEMA", "STAGING"),
        },
        "slack_webhook": os.environ["SLACK_WEBHOOK_URL"],
        "airflow_url":   os.environ.get("AIRFLOW_API_URL"),
    }


def run_data_quality_checks(
    monitor: SnowflakeMonitor,
    engine: DiagnosisEngine,
    notifier: SlackNotifier,
) -> None:
    """Run DQ checks on all monitored tables and alert on anomalies."""
    for table, columns in TABLES_TO_MONITOR:
        logger.info("Checking data quality: %s", table)
        try:
            anomalies = monitor.check_table(table, columns)
            for anomaly in anomalies:
                dq_context = DataQualityContext(
                    table_name=anomaly.table,
                    column_name=anomaly.column or "N/A",
                    issue_type=anomaly.issue_type,
                    current_value=anomaly.current_value,
                    baseline_value=anomaly.baseline_value,
                    deviation_pct=anomaly.deviation_pct,
                    sample_rows=anomaly.sample_rows,
                )
                diagnosis = engine.diagnose_data_quality_issue(dq_context)

                if diagnosis.alert_score >= ALERT_SCORE_THRESHOLD:
                    logger.info(
                        "Sending DQ alert: table=%s issue=%s score=%d",
                        table, anomaly.issue_type, diagnosis.alert_score,
                    )
                    notifier.send_data_quality_alert(dq_context, diagnosis)
                else:
                    logger.info(
                        "DQ anomaly suppressed (score=%d < threshold=%d): %s %s",
                        diagnosis.alert_score, ALERT_SCORE_THRESHOLD,
                        table, anomaly.issue_type,
                    )
        except Exception as exc:
            logger.error("DQ check error for %s: %s", table, exc)


def main() -> None:
    config = load_config()

    engine = DiagnosisEngine(api_key=config["anthropic_api_key"])
    monitor = SnowflakeMonitor(**config["snowflake"])
    notifier = SlackNotifier(webhook_url=config["slack_webhook"])

    logger.info("Agent started. Poll interval: %ds", POLL_INTERVAL_SECONDS)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        func=lambda: run_data_quality_checks(monitor, engine, notifier),
        trigger="interval",
        seconds=POLL_INTERVAL_SECONDS,
        next_run_time=datetime.now(),
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Agent stopped.")


if __name__ == "__main__":
    main()
