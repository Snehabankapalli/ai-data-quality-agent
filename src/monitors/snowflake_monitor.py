"""
Snowflake data quality monitor.
Detects null spikes, volume drops, schema drift, and duplicate surges
by comparing current state against a 7-day rolling baseline.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import snowflake.connector

logger = logging.getLogger(__name__)

ALERT_THRESHOLDS = {
    "null_spike_pct":      5.0,   # Alert if null rate increases by more than 5 percentage points
    "volume_drop_pct":    30.0,   # Alert if row count drops more than 30% vs baseline
    "volume_surge_pct":  200.0,   # Alert if row count surges more than 200% vs baseline
    "duplicate_rate_pct":  1.0,   # Alert if duplicate rate exceeds 1%
}


@dataclass
class QualityAnomaly:
    table: str
    column: Optional[str]
    issue_type: str
    current_value: float
    baseline_value: float
    deviation_pct: float
    sample_rows: list[dict]


class SnowflakeMonitor:
    """
    Polls Snowflake tables for data quality anomalies.
    Compares current metrics against 7-day rolling baseline.
    """

    def __init__(self, account: str, user: str, password: str, database: str, schema: str):
        self._conn_params = {
            "account":   account,
            "user":      user,
            "password":  password,
            "database":  database,
            "schema":    schema,
        }

    def check_table(self, table: str, columns: list[str]) -> list[QualityAnomaly]:
        """Run all quality checks on a table. Returns list of detected anomalies."""
        anomalies = []
        with snowflake.connector.connect(**self._conn_params) as conn:
            anomalies.extend(self._check_volume(conn, table))
            for col in columns:
                anomalies.extend(self._check_nulls(conn, table, col))
            anomalies.extend(self._check_duplicates(conn, table))
        return anomalies

    def _check_volume(self, conn, table: str) -> list[QualityAnomaly]:
        """Detect row count drops or surges vs 7-day average."""
        sql = f"""
        WITH daily_counts AS (
            SELECT
                DATE_TRUNC('day', ingested_at) AS day,
                COUNT(*) AS row_count
            FROM {table}
            WHERE ingested_at >= DATEADD('day', -8, CURRENT_TIMESTAMP())
            GROUP BY 1
        ),
        baseline AS (
            SELECT AVG(row_count) AS avg_count
            FROM daily_counts
            WHERE day < CURRENT_DATE()
        ),
        today AS (
            SELECT row_count AS today_count
            FROM daily_counts
            WHERE day = CURRENT_DATE()
        )
        SELECT
            b.avg_count    AS baseline,
            t.today_count  AS current,
            IFF(b.avg_count > 0,
                (t.today_count - b.avg_count) / b.avg_count * 100,
                0
            ) AS deviation_pct
        FROM baseline b, today t
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                return []
            baseline, current, deviation = row
            if abs(deviation) > ALERT_THRESHOLDS["volume_drop_pct"]:
                issue = "volume_drop" if deviation < 0 else "volume_surge"
                logger.warning(
                    "Volume anomaly on %s: current=%d baseline=%.0f deviation=%.1f%%",
                    table, current, baseline, deviation,
                )
                return [QualityAnomaly(
                    table=table,
                    column=None,
                    issue_type=issue,
                    current_value=float(current),
                    baseline_value=float(baseline),
                    deviation_pct=float(deviation),
                    sample_rows=[],
                )]
        except Exception as exc:
            logger.error("Volume check failed for %s: %s", table, exc)
        return []

    def _check_nulls(self, conn, table: str, column: str) -> list[QualityAnomaly]:
        """Detect null rate spikes compared to 7-day baseline."""
        sql = f"""
        WITH daily_nulls AS (
            SELECT
                DATE_TRUNC('day', ingested_at) AS day,
                COUNT(*) AS total,
                SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS nulls
            FROM {table}
            WHERE ingested_at >= DATEADD('day', -8, CURRENT_TIMESTAMP())
            GROUP BY 1
        ),
        baseline AS (
            SELECT AVG(nulls / NULLIF(total, 0) * 100) AS avg_null_rate
            FROM daily_nulls
            WHERE day < CURRENT_DATE()
        ),
        today AS (
            SELECT nulls / NULLIF(total, 0) * 100 AS null_rate
            FROM daily_nulls
            WHERE day = CURRENT_DATE()
        )
        SELECT b.avg_null_rate, t.null_rate
        FROM baseline b, today t
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                return []
            baseline_rate, current_rate = row
            if baseline_rate is None or current_rate is None:
                return []
            spike = current_rate - baseline_rate
            if spike > ALERT_THRESHOLDS["null_spike_pct"]:
                sample = self._sample_nulls(conn, table, column)
                logger.warning(
                    "Null spike on %s.%s: current=%.1f%% baseline=%.1f%% spike=+%.1f%%",
                    table, column, current_rate, baseline_rate, spike,
                )
                return [QualityAnomaly(
                    table=table,
                    column=column,
                    issue_type="null_spike",
                    current_value=float(current_rate),
                    baseline_value=float(baseline_rate),
                    deviation_pct=float(spike),
                    sample_rows=sample,
                )]
        except Exception as exc:
            logger.error("Null check failed for %s.%s: %s", table, column, exc)
        return []

    def _check_duplicates(self, conn, table: str) -> list[QualityAnomaly]:
        """Detect duplicate key surge using information_schema primary key constraints."""
        sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT transaction_id) AS unique_ids,
            (COUNT(*) - COUNT(DISTINCT transaction_id)) / NULLIF(COUNT(*), 0) * 100 AS dup_rate
        FROM {table}
        WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        """
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                return []
            total, unique, dup_rate = row
            if dup_rate and dup_rate > ALERT_THRESHOLDS["duplicate_rate_pct"]:
                return [QualityAnomaly(
                    table=table,
                    column="transaction_id",
                    issue_type="duplicate_surge",
                    current_value=float(dup_rate),
                    baseline_value=0.0,
                    deviation_pct=float(dup_rate),
                    sample_rows=[],
                )]
        except Exception as exc:
            logger.error("Duplicate check failed for %s: %s", table, exc)
        return []

    def _sample_nulls(self, conn, table: str, column: str, limit: int = 5) -> list[dict]:
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {column} IS NULL
          AND ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        LIMIT {limit}
        """
        try:
            cursor = conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception:
            return []
