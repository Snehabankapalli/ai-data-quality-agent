"""
DiagnosisEngine: uses Claude API to diagnose pipeline failures.
Takes structured context (logs, schema diffs, row counts) and returns
a structured diagnosis with root cause, severity, SQL fix, and auto-heal flag.
"""

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import anthropic

from src.llm.prompts import build_diagnosis_prompt, build_data_quality_prompt

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 2048


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Confidence(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass
class Diagnosis:
    root_cause: str
    severity: Severity
    confidence: Confidence
    fix_description: str
    sql_fix: Optional[str]
    auto_healable: bool
    auto_heal_action: Optional[str]
    alert_score: int  # 0-100, used for alert fatigue filtering


@dataclass
class PipelineContext:
    pipeline_name: str
    task_name: str
    error_message: str
    stack_trace: str
    task_history: list[dict]       # Last 10 task states
    upstream_row_counts: dict      # table → row count
    schema_diff: Optional[dict]    # None if no schema change
    recent_commits: list[str]      # Last 3 git commit messages
    failure_count_24h: int


@dataclass
class DataQualityContext:
    table_name: str
    column_name: str
    issue_type: str               # null_spike, volume_drop, schema_drift, duplicate_surge
    current_value: float
    baseline_value: float
    deviation_pct: float
    sample_rows: list[dict]


class DiagnosisEngine:
    """
    Wraps Claude API for structured pipeline failure diagnosis.
    All responses are parsed into typed Diagnosis objects.
    """

    def __init__(self, api_key: str):
        self._client = anthropic.Anthropic(api_key=api_key)

    def diagnose_pipeline_failure(self, context: PipelineContext) -> Diagnosis:
        """
        Send pipeline failure context to Claude and return structured diagnosis.
        """
        prompt = build_diagnosis_prompt(context)

        logger.info(
            "Diagnosing failure: pipeline=%s task=%s",
            context.pipeline_name,
            context.task_name,
        )

        response = self._client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=(
                "You are a senior data engineer diagnosing production pipeline failures. "
                "You have deep expertise in Apache Airflow, AWS Glue, PySpark, Snowflake, and dbt. "
                "Always respond with valid JSON matching the schema provided. "
                "Be specific and actionable — vague diagnoses are useless to on-call engineers."
            ),
            messages=[{"role": "user", "content": prompt}],
        )

        return self._parse_pipeline_diagnosis(response.content[0].text, context)

    def diagnose_data_quality_issue(self, context: DataQualityContext) -> Diagnosis:
        """
        Diagnose a data quality anomaly (null spike, volume drop, schema drift).
        """
        prompt = build_data_quality_prompt(context)

        logger.info(
            "Diagnosing data quality issue: table=%s column=%s issue=%s",
            context.table_name,
            context.column_name,
            context.issue_type,
        )

        response = self._client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=(
                "You are a data quality engineer diagnosing anomalies in production data pipelines. "
                "You specialize in Snowflake, dbt, and SQL. "
                "Respond only with valid JSON. Be specific about root cause and provide runnable SQL."
            ),
            messages=[{"role": "user", "content": prompt}],
        )

        return self._parse_dq_diagnosis(response.content[0].text, context)

    def _parse_pipeline_diagnosis(self, raw: str, context: PipelineContext) -> Diagnosis:
        """Parse Claude's JSON response into a typed Diagnosis."""
        try:
            data = json.loads(self._extract_json(raw))
            return Diagnosis(
                root_cause=data["root_cause"],
                severity=Severity(data.get("severity", "MEDIUM")),
                confidence=Confidence(data.get("confidence", "MEDIUM")),
                fix_description=data["fix_description"],
                sql_fix=data.get("sql_fix"),
                auto_healable=data.get("auto_healable", False) and context.failure_count_24h < 3,
                auto_heal_action=data.get("auto_heal_action"),
                alert_score=int(data.get("alert_score", 75)),
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("Failed to parse Claude diagnosis response: %s", exc)
            return Diagnosis(
                root_cause=f"Parse error — raw response: {raw[:200]}",
                severity=Severity.MEDIUM,
                confidence=Confidence.LOW,
                fix_description="Manual investigation required.",
                sql_fix=None,
                auto_healable=False,
                auto_heal_action=None,
                alert_score=80,
            )

    def _parse_dq_diagnosis(self, raw: str, context: DataQualityContext) -> Diagnosis:
        try:
            data = json.loads(self._extract_json(raw))
            return Diagnosis(
                root_cause=data["root_cause"],
                severity=Severity(data.get("severity", "MEDIUM")),
                confidence=Confidence(data.get("confidence", "MEDIUM")),
                fix_description=data["fix_description"],
                sql_fix=data.get("sql_fix"),
                auto_healable=data.get("auto_healable", False),
                auto_heal_action=data.get("auto_heal_action"),
                alert_score=int(data.get("alert_score", 65)),
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("Failed to parse Claude DQ response: %s", exc)
            return Diagnosis(
                root_cause="Parse error — manual review needed",
                severity=Severity.MEDIUM,
                confidence=Confidence.LOW,
                fix_description="Check table manually.",
                sql_fix=None,
                auto_healable=False,
                auto_heal_action=None,
                alert_score=70,
            )

    @staticmethod
    def _extract_json(text: str) -> str:
        """Extract JSON from Claude response (handles markdown code blocks)."""
        if "```json" in text:
            start = text.index("```json") + 7
            end = text.index("```", start)
            return text[start:end].strip()
        if "```" in text:
            start = text.index("```") + 3
            end = text.index("```", start)
            return text[start:end].strip()
        return text.strip()
