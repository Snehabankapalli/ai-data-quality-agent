"""Tests for prompt templates — no API calls required."""

import json
import pytest

from src.llm.prompts import build_diagnosis_prompt, build_data_quality_prompt


class TestBuildDiagnosisPrompt:
    def test_contains_pipeline_name(self, pipeline_context):
        prompt = build_diagnosis_prompt(pipeline_context)
        assert pipeline_context.pipeline_name in prompt

    def test_contains_task_name(self, pipeline_context):
        prompt = build_diagnosis_prompt(pipeline_context)
        assert pipeline_context.task_name in prompt

    def test_contains_error_message(self, pipeline_context):
        prompt = build_diagnosis_prompt(pipeline_context)
        assert pipeline_context.error_message in prompt

    def test_contains_failure_count(self, pipeline_context):
        prompt = build_diagnosis_prompt(pipeline_context)
        assert str(pipeline_context.failure_count_24h) in prompt

    def test_contains_json_schema_instructions(self, pipeline_context):
        prompt = build_diagnosis_prompt(pipeline_context)
        assert "root_cause" in prompt
        assert "severity" in prompt
        assert "auto_healable" in prompt
        assert "alert_score" in prompt

    def test_stack_trace_truncated_at_2000_chars(self):
        from tests.conftest import *
        import pytest
        ctx_dict = {
            "pipeline_name": "p",
            "task_name": "t",
            "error_message": "err",
            "stack_trace": "X" * 5000,
            "task_history": [],
            "upstream_row_counts": {},
            "schema_diff": None,
            "recent_commits": [],
            "failure_count_24h": 0,
        }
        from src.agent.diagnosis_engine import PipelineContext
        ctx = PipelineContext(**ctx_dict)
        prompt = build_diagnosis_prompt(ctx)
        # Stack trace is sliced to 2000 chars in prompt
        assert "X" * 2000 in prompt
        assert "X" * 2001 not in prompt

    def test_schema_diff_none_renders_empty_dict(self, pipeline_context):
        from src.agent.diagnosis_engine import PipelineContext
        import dataclasses
        ctx = dataclasses.replace(pipeline_context, schema_diff=None)
        prompt = build_diagnosis_prompt(ctx)
        assert "{}" in prompt

    def test_recent_commits_listed(self, pipeline_context):
        prompt = build_diagnosis_prompt(pipeline_context)
        for commit in pipeline_context.recent_commits:
            assert commit in prompt


class TestBuildDataQualityPrompt:
    def test_contains_table_name(self, dq_context):
        prompt = build_data_quality_prompt(dq_context)
        assert dq_context.table_name in prompt

    def test_contains_column_name(self, dq_context):
        prompt = build_data_quality_prompt(dq_context)
        assert dq_context.column_name in prompt

    def test_contains_issue_type(self, dq_context):
        prompt = build_data_quality_prompt(dq_context)
        assert dq_context.issue_type in prompt

    def test_contains_deviation(self, dq_context):
        prompt = build_data_quality_prompt(dq_context)
        assert f"{dq_context.deviation_pct:.1f}%" in prompt

    def test_contains_json_schema(self, dq_context):
        prompt = build_data_quality_prompt(dq_context)
        assert "root_cause" in prompt
        assert "sql_fix" in prompt

    def test_sample_rows_included(self, dq_context):
        prompt = build_data_quality_prompt(dq_context)
        assert "transaction_id" in prompt
