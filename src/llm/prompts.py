"""
Structured prompt templates for the AI diagnosis agent.
Prompts are designed to produce consistent, parseable JSON responses from Claude.
"""

import json


def build_diagnosis_prompt(context) -> str:
    return f"""
You are diagnosing a production data pipeline failure. Analyze the context below and respond with a JSON object.

## Failure Context

**Pipeline:** {context.pipeline_name}
**Failed Task:** {context.task_name}
**Error Message:** {context.error_message}

**Stack Trace:**
```
{context.stack_trace[:2000]}
```

**Recent Task History (last 10 runs):**
```json
{json.dumps(context.task_history, indent=2)}
```

**Upstream Table Row Counts:**
```json
{json.dumps(context.upstream_row_counts, indent=2)}
```

**Schema Changes Since Last Successful Run:**
```json
{json.dumps(context.schema_diff or {}, indent=2)}
```

**Recent Git Commits (last 3):**
{chr(10).join(f"- {c}" for c in context.recent_commits)}

**Times Failed in Last 24 Hours:** {context.failure_count_24h}

---

## Instructions

Diagnose the root cause. Consider: schema changes, upstream data issues, resource constraints,
code bugs, network failures, and Snowflake warehouse issues.

Respond ONLY with this JSON structure (no extra text):

```json
{{
  "root_cause": "One specific sentence describing the root cause",
  "severity": "LOW | MEDIUM | HIGH | CRITICAL",
  "confidence": "LOW | MEDIUM | HIGH",
  "fix_description": "Step-by-step numbered list of what to do",
  "sql_fix": "Runnable SQL to verify or fix the issue, or null if not applicable",
  "auto_healable": true or false,
  "auto_heal_action": "e.g. 'rerun task' or null if not auto-healable",
  "alert_score": 0-100
}}
```

**Alert score guide:** 0=noise, 100=P0 incident. Base on: business impact, deviation severity, time of day.
**Auto-healable:** only true for transient failures (network timeout, warehouse suspend). Never true for data bugs.
"""


def build_data_quality_prompt(context) -> str:
    return f"""
You are diagnosing a data quality anomaly in a production Snowflake table. Analyze and respond with JSON.

## Anomaly Context

**Table:** {context.table_name}
**Column:** {context.column_name}
**Issue Type:** {context.issue_type}
**Current Value:** {context.current_value}
**7-Day Baseline:** {context.baseline_value}
**Deviation:** {context.deviation_pct:.1f}%

**Sample Affected Rows:**
```json
{json.dumps(context.sample_rows[:5], indent=2)}
```

---

## Instructions

Identify the root cause. Common causes: upstream schema change, new data source, pipeline bug,
Fiserv/vendor payload change, dbt macro bug, missing WHERE clause, bad join.

Respond ONLY with this JSON:

```json
{{
  "root_cause": "One specific sentence",
  "severity": "LOW | MEDIUM | HIGH | CRITICAL",
  "confidence": "LOW | MEDIUM | HIGH",
  "fix_description": "Numbered steps to fix",
  "sql_fix": "Runnable SQL to diagnose or fix the issue",
  "auto_healable": false,
  "auto_heal_action": null,
  "alert_score": 0-100
}}
```
"""
