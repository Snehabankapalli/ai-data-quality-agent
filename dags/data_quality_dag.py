"""
Airflow DAG for scheduled data quality monitoring.
Runs the AI agent on a configurable schedule (default: every 30 minutes).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def run_data_quality_agent(**context):
    """Airflow task: run the full data quality agent pipeline."""
    import sys
    import os

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    from src.agent import DataQualityAgent

    agent = DataQualityAgent()
    result = agent.run()

    context["task_instance"].xcom_push(key="agent_result", value=result)

    if result.get("severity") in ("critical", "high") and not result.get("auto_healed"):
        raise ValueError(
            f"Unresolved {result['severity']} data quality failure. "
            "Manual intervention required."
        )

    return result


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ai_data_quality_agent",
    description="Claude-powered data quality monitoring for Snowflake pipelines",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",   # Every 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "ai-agent", "snowflake", "fintech"],
) as dag:

    run_checks = PythonOperator(
        task_id="run_data_quality_agent",
        python_callable=run_data_quality_agent,
        doc_md="""
        Runs the AI data quality agent:
        1. Executes freshness, completeness, and anomaly checks
        2. Sends failures to Claude for root cause analysis
        3. Triggers auto-healing if recommended
        4. Sends Slack alert with Claude's analysis
        """,
    )
