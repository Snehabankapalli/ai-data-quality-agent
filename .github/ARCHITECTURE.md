# AI Data Quality Agent — Architecture

LLM-powered data validation agent with multi-warehouse monitoring.

## System Flow

```mermaid
graph LR
    A["📅 Scheduler<br/>cron / Airflow"]
    B["🤖 Validation Agent<br/>LLM reasoning loop"]
    C["🔍 Monitors<br/>- Snowflake<br/>- BigQuery<br/>- Redshift"]
    D["🛠 Tools<br/>- row_count<br/>- null_check<br/>- freshness<br/>- schema_drift"]
    E["📊 Report<br/>severity-ranked"]
    F["💬 Slack alert<br/>P1 only"]

    A -->|trigger| B
    B -->|plan checks| C
    C -->|run SQL| D
    D -->|findings| B
    B -->|synthesize| E
    E -->|P1| F

    style B fill:#f3e5f5
    style E fill:#e8f5e9
    style F fill:#fce4ec
```

## Monitor Abstraction

```mermaid
graph TB
    subgraph "Interface"
        I["DataQualityMonitor<br/>check_table(table, rules)"]
    end

    subgraph "Implementations"
        S["SnowflakeMonitor<br/>snowflake-connector"]
        B["BigQueryMonitor<br/>google-cloud-bigquery"]
        R["RedshiftMonitor<br/>psycopg2"]
    end

    subgraph "Checks"
        C1["row_count<br/>threshold_pct"]
        C2["null_check<br/>max_null_pct"]
        C3["freshness<br/>max_age_hours"]
        C4["duplicates<br/>on_columns"]
        C5["schema_drift<br/>vs baseline"]
    end

    I --> S
    I --> B
    I --> R
    S --> C1
    S --> C2
    S --> C3
    S --> C4
    S --> C5
    B --> C1
    B --> C2
    B --> C3
    R --> C1
    R --> C2
    R --> C3
    R --> C4

    style I fill:#fff3e0
```

## LLM Agent Loop

```mermaid
sequenceDiagram
    participant U as User/Scheduler
    participant A as Validation Agent
    participant L as Claude (Sonnet 4.6)
    participant M as Monitor
    participant W as Warehouse

    U->>A: validate(table, context)
    A->>L: plan checks for table
    L-->>A: JSON: [check_list]
    loop per check
        A->>M: execute(check)
        M->>W: SELECT query
        W-->>M: result
        M-->>A: finding
    end
    A->>L: synthesize findings + severity
    L-->>A: ranked report + fix suggestions
    A-->>U: DataQualityReport
```

## Severity Model

| Severity | Trigger | Action |
|----------|---------|--------|
| **P1 CRITICAL** | >10% null on PK, stale >24h, schema drift on PROD | Page on-call + Slack |
| **P2 HIGH** | 1-10% null, stale 12-24h, unexpected row delta | Slack warning |
| **P3 MEDIUM** | <1% null, stale 6-12h | Log + daily digest |
| **P4 INFO** | Baseline drift, new column | Dashboard only |

## Tech Stack

| Layer | Technology |
|-------|------------|
| Agent | Python 3.11, Anthropic SDK, claude-sonnet-4-6 |
| Warehouses | Snowflake, BigQuery, Redshift |
| Orchestration | cron / Airflow / cloud scheduler |
| Alerts | Slack webhooks |
| Storage | JSON reports, Prometheus metrics |
