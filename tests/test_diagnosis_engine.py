"""Tests for DiagnosisEngine — Claude API wrapper."""

import json
import pytest
from unittest.mock import MagicMock, patch

from src.agent.diagnosis_engine import (
    DiagnosisEngine,
    Diagnosis,
    Severity,
    Confidence,
    PipelineContext,
    DataQualityContext,
)


class TestDiagnosisEngineExtractJson:
    def test_plain_json(self):
        raw = '{"root_cause": "schema drift"}'
        assert DiagnosisEngine._extract_json(raw) == '{"root_cause": "schema drift"}'

    def test_json_in_markdown_block(self):
        raw = '```json\n{"root_cause": "schema drift"}\n```'
        result = DiagnosisEngine._extract_json(raw)
        assert result == '{"root_cause": "schema drift"}'

    def test_json_in_plain_code_block(self):
        raw = '```\n{"key": "value"}\n```'
        result = DiagnosisEngine._extract_json(raw)
        assert result == '{"key": "value"}'

    def test_strips_whitespace(self):
        raw = '  {"key": "value"}  '
        assert DiagnosisEngine._extract_json(raw) == '{"key": "value"}'


class TestDiagnosisEngineParsePipeline:
    def setup_method(self):
        with patch("anthropic.Anthropic"):
            self.engine = DiagnosisEngine(api_key="test-key")

    def test_valid_response_returns_diagnosis(self, pipeline_context, mock_anthropic_response):
        with patch.object(self.engine._client.messages, "create", return_value=mock_anthropic_response):
            result = self.engine.diagnose_pipeline_failure(pipeline_context)

        assert isinstance(result, Diagnosis)
        assert result.severity == Severity.HIGH
        assert result.confidence == Confidence.HIGH
        assert result.alert_score == 88
        assert result.auto_healable is False

    def test_auto_heal_suppressed_when_too_many_failures(self, mock_anthropic_response):
        with patch("anthropic.Anthropic"):
            engine = DiagnosisEngine(api_key="test-key")

        ctx = PipelineContext(
            pipeline_name="test_pipeline",
            task_name="task_a",
            error_message="timeout",
            stack_trace="",
            task_history=[],
            upstream_row_counts={},
            schema_diff=None,
            recent_commits=[],
            failure_count_24h=5,  # >= 3 suppresses auto-heal
        )

        # Response says auto_healable: true, but failure_count >= 3 should suppress
        msg = MagicMock()
        msg.content = [MagicMock()]
        msg.content[0].text = json.dumps({
            "root_cause": "transient network timeout",
            "severity": "LOW",
            "confidence": "HIGH",
            "fix_description": "Rerun the task.",
            "sql_fix": None,
            "auto_healable": True,
            "auto_heal_action": "rerun task",
            "alert_score": 40,
        })

        with patch.object(engine._client.messages, "create", return_value=msg):
            result = engine.diagnose_pipeline_failure(ctx)

        assert result.auto_healable is False

    def test_malformed_json_returns_fallback(self, pipeline_context):
        with patch("anthropic.Anthropic"):
            engine = DiagnosisEngine(api_key="test-key")

        msg = MagicMock()
        msg.content = [MagicMock()]
        msg.content[0].text = "NOT VALID JSON AT ALL"

        with patch.object(engine._client.messages, "create", return_value=msg):
            result = engine.diagnose_pipeline_failure(pipeline_context)

        assert result.severity == Severity.MEDIUM
        assert result.confidence == Confidence.LOW
        assert result.auto_healable is False
        assert "Parse error" in result.root_cause

    def test_missing_required_field_returns_fallback(self, pipeline_context):
        with patch("anthropic.Anthropic"):
            engine = DiagnosisEngine(api_key="test-key")

        msg = MagicMock()
        msg.content = [MagicMock()]
        msg.content[0].text = json.dumps({"severity": "HIGH"})  # missing root_cause

        with patch.object(engine._client.messages, "create", return_value=msg):
            result = engine.diagnose_pipeline_failure(pipeline_context)

        assert result.confidence == Confidence.LOW


class TestDiagnosisEngineParseDataQuality:
    def setup_method(self):
        with patch("anthropic.Anthropic"):
            self.engine = DiagnosisEngine(api_key="test-key")

    def test_valid_dq_response(self, dq_context):
        msg = MagicMock()
        msg.content = [MagicMock()]
        msg.content[0].text = json.dumps({
            "root_cause": "Upstream vendor stopped sending merchant_id.",
            "severity": "HIGH",
            "confidence": "HIGH",
            "fix_description": "1. Check vendor API. 2. Add fallback.",
            "sql_fix": "SELECT COUNT(*) FROM STAGING.STG_CARD_TRANSACTIONS WHERE merchant_id IS NULL;",
            "auto_healable": False,
            "auto_heal_action": None,
            "alert_score": 75,
        })

        with patch.object(self.engine._client.messages, "create", return_value=msg):
            result = self.engine.diagnose_data_quality_issue(dq_context)

        assert isinstance(result, Diagnosis)
        assert result.severity == Severity.HIGH
        assert result.sql_fix is not None

    def test_dq_fallback_on_parse_error(self, dq_context):
        msg = MagicMock()
        msg.content = [MagicMock()]
        msg.content[0].text = "malformed"

        with patch.object(self.engine._client.messages, "create", return_value=msg):
            result = self.engine.diagnose_data_quality_issue(dq_context)

        assert result.confidence == Confidence.LOW
        assert result.auto_healable is False


class TestSeverityEnum:
    def test_all_levels_exist(self):
        assert Severity.LOW == "LOW"
        assert Severity.MEDIUM == "MEDIUM"
        assert Severity.HIGH == "HIGH"
        assert Severity.CRITICAL == "CRITICAL"

    def test_from_string(self):
        assert Severity("HIGH") == Severity.HIGH


class TestConfidenceEnum:
    def test_all_levels_exist(self):
        assert Confidence.LOW == "LOW"
        assert Confidence.MEDIUM == "MEDIUM"
        assert Confidence.HIGH == "HIGH"
