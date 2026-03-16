# AI Data Quality Agent

> A Claude-powered data quality monitoring bot for Snowflake pipelines — with self-healing and Slack alerting.

Built by a Senior Data Engineer who has processed **100M+ daily events** at SoFi with **99.9% pipeline SLA**. This agent brings fintech-grade reliability to any data team.

---

## What It Does

```
Snowflake Tables
      │
      ▼
┌─────────────────────────────────┐
│     Data Quality Checks         │
│  ┌──────────┐ ┌──────────────┐  │
│  │Freshness │ │ Completeness │  │
│  └──────────┘ └──────────────┘  │
│       ┌────────────────┐        │
│       │ Anomaly (z-score)│       │
│       └────────────────┘        │
└──────────────┬──────────────────┘
               │ failures
               ▼
     ┌─────────────────┐
     │  Claude Opus    │  ← root cause analysis
     │  (AI Agent)     │    auto-heal decision
     └────────┬────────┘    Slack message draft
              │
     ┌────────┴────────┐
     │                 │
     ▼                 ▼
┌─────────┐    ┌──────────────┐
│  Slack  │    │ Self-Healing │
│  Alert  │    │ (dbt / MV)   │
└─────────┘    └──────────────┘
```

**Three check types:**
- **Freshness** — validates tables are loaded within SLA windows (e.g., 4h for credit card transactions)
- **Completeness** — flags null rates exceeding thresholds on critical columns
- **Anomaly** — z-score detection on row counts and metrics over rolling windows

**Claude's role:** analyzes all failures together, determines root cause, writes the Slack message, and decides whether to trigger auto-healing — so your engineers wake up to context, not raw metrics.

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/Snehabankapalli/ai-data-quality-agent.git
cd ai-data-quality-agent
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in your Snowflake credentials, Anthropic API key, and Slack bot token
```

### 3. Edit your checks

```yaml
# config/checks.yaml
checks:
  freshness:
    - table: TRANSACTIONS
      max_hours_since_load: 4
      severity: critical
```

### 4. Run

```bash
python -m src.agent
```

Or deploy via Airflow — the DAG runs every 30 minutes by default:

```bash
cp dags/data_quality_dag.py $AIRFLOW_HOME/dags/
airflow dags trigger ai_data_quality_agent
```

---

## Sample Slack Alert

```
🚨 Data Quality Alert: Stale pipeline — TRANSACTIONS not refreshed

Severity:   CRITICAL
Check:      TRANSACTIONS loaded 7.2h ago (SLA: 4h, rows: 0)

🤖 Claude's Analysis:
The TRANSACTIONS table has not been updated in 7.2 hours, exceeding the 4-hour
SLA required for same-day credit reporting. The anomaly detector also flagged
a 94% drop in row count. Root cause is likely a stalled Snowpipe or upstream
payment processor API timeout.

Recommended Action:
> Resume the TRANSACTIONS Snowpipe and verify Fiserv API connectivity.
  If unresolved in 15 minutes, escalate to on-call.

Auto-Healing: ✅ Triggered dbt run for stg_transactions
```

---

## Project Structure

```
ai-data-quality-agent/
├── src/
│   ├── agent.py              # Main orchestrator (Claude + checks + healing)
│   ├── snowflake_client.py   # Snowflake queries with retry logic
│   ├── slack_notifier.py     # Block Kit Slack messages
│   ├── checks/
│   │   ├── freshness.py      # SLA window validation
│   │   ├── completeness.py   # Null rate analysis
│   │   └── anomaly.py        # Z-score anomaly detection
│   └── healing/
│       └── remediation.py    # dbt runs, MV refresh, Snowpipe resume
├── config/
│   └── checks.yaml           # All check configuration
├── dags/
│   └── data_quality_dag.py   # Airflow DAG (30-min schedule)
├── tests/
│   └── test_checks.py        # Unit tests with mocked Snowflake
├── .env.example
└── requirements.txt
```

---

## Configuration Reference

### Freshness Check

| Field | Description |
|---|---|
| `table` | Snowflake table name |
| `max_hours_since_load` | Maximum hours before flagging stale |
| `severity` | `low` / `medium` / `high` / `critical` |

### Completeness Check

| Field | Description |
|---|---|
| `table` | Snowflake table name |
| `columns[].name` | Column to check |
| `columns[].max_null_pct` | Max allowed null % (0.0 = zero nulls) |

### Anomaly Check

| Field | Description |
|---|---|
| `metric` | `row_count` or `avg_amount` |
| `window_days` | Rolling baseline window |
| `z_score_threshold` | Std devs before flagging (default: 3.0) |
| `min_expected_rows` | Hard minimum regardless of z-score |

---

## Healing Actions

Claude can trigger these automatically when confidence is high:

| Action | When Used |
|---|---|
| `refresh_materialized_view` | MV is stale but source data is fresh |
| `trigger_dbt_run` | Incremental model missed a run |
| `resume_snowpipe` | Snowpipe paused due to transient error |

---

## Tech Stack

`Python 3.11` · `Anthropic Claude Opus` · `Snowflake` · `dbt` · `Airflow` · `Slack SDK` · `Pandas` · `SciPy`

---

## About

Built by **Sneha Bankapalli** — Senior Data Engineer with 7+ years at SoFi, Optum/UHG, and T-Mobile.
This agent is based on patterns used in production fintech pipelines processing 100M+ daily events.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-sneha2095-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/sneha2095/)
