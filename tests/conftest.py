"""Shared fixtures for agent tests."""

import pytest
from unittest.mock import MagicMock

from src.agent.diagnosis_engine import (
    Diagnosis,
    Severity,
    Confidence,
    PipelineContext,
    DataQualityContext,
)


@pytest.fixture
def pipeline_context():
    return PipelineContext(
        pipeline_name="fintech_daily_load",
        task_name="load_card_transactions",
        error_message="SnowflakeError: Column 'merchant_id' not found",
        stack_trace="Traceback (most recent call last):\n  File 'glue_job.py', line 42",
        task_history=[{"state": "success"}, {"state": "success"}, {"state": "failed"}],
        upstream_row_counts={"STAGING.STG_CARDS": 100_000, "STAGING.STG_MERCHANTS": 5_000},
        schema_diff={"added": [], "removed": ["merchant_id"], "modified": []},
        recent_commits=["feat: add merchant_id to staging model", "fix: null handling"],
        failure_count_24h=1,
    )


@pytest.fixture
def dq_context():
    return DataQualityContext(
        table_name="STAGING.STG_CARD_TRANSACTIONS",
        column_name="merchant_id",
        issue_type="null_spike",
        current_value=35.2,
        baseline_value=1.1,
        deviation_pct=34.1,
        sample_rows=[{"transaction_id": "abc", "merchant_id": None}],
    )


@pytest.fixture
def diagnosis_high():
    return Diagnosis(
        root_cause="Schema migration removed merchant_id column from upstream source.",
        severity=Severity.HIGH,
        confidence=Confidence.HIGH,
        fix_description="1. Roll back migration. 2. Add merchant_id back to schema.",
        sql_fix="SELECT * FROM STAGING.STG_CARD_TRANSACTIONS WHERE merchant_id IS NULL LIMIT 10;",
        auto_healable=False,
        auto_heal_action=None,
        alert_score=88,
    )


@pytest.fixture
def mock_anthropic_response():
    msg = MagicMock()
    msg.content = [MagicMock()]
    msg.content[0].text = """{
        "root_cause": "Schema drift removed merchant_id from upstream table.",
        "severity": "HIGH",
        "confidence": "HIGH",
        "fix_description": "1. Identify the offending migration. 2. Roll back.",
        "sql_fix": "SELECT * FROM STAGING.STG_CARD_TRANSACTIONS WHERE merchant_id IS NULL LIMIT 5;",
        "auto_healable": false,
        "auto_heal_action": null,
        "alert_score": 88
    }"""
    return msg
