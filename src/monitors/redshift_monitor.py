"""
Amazon Redshift data quality monitor.

Detects null spikes, volume anomalies, schema drift, and stale data
in Redshift tables. Same anomaly interface as SnowflakeMonitor and
BigQueryMonitor for consistent handling in the agent core.

Usage:
    monitor = RedshiftMonitor(
        host="my-cluster.us-east-1.redshift.amazonaws.com",
        database="analytics",
        user="pipeline_user",
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    anomalies = monitor.check_table("events", columns=["user_id", "event_type"])
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

ALERT_THRESHOLDS = {
    "null_spike_pct":     5.0,
    "volume_drop_pct":   30.0,
    "volume_surge_pct": 200.0,
    "duplicate_rate_pct": 1.0,
    "freshness_hours":    24.0,
}


@dataclass
class QualityAnomaly:
    """Data quality anomaly from Redshift monitor."""
    table: str
    column: Optional[str]
    issue_type: str
    current_value: float
    baseline_value: float
    deviation_pct: float
    sample_rows: list


class RedshiftMonitor:
    """
    Polls Redshift tables for data quality anomalies using psycopg2.
    Leverages SVV_TABLE_INFO and STL_LOAD_COMMITS for metadata queries.

    Requires psycopg2-binary package.
    """

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5439,
        schema: str = "public",
    ):
        self._conn_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
        }
        self._schema = schema

    def _get_connection(self):
        """Create a new Redshift connection. Caller is responsible for closing."""
        import psycopg2
        return psycopg2.connect(**self._conn_params)

    def _execute(self, query: str, params=None) -> list[dict]:
        """Execute query and return results as list of dicts."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    def check_table(self, table: str, columns: list[str]) -> list[QualityAnomaly]:
        """
        Run all quality checks on a Redshift table.
        Returns list of anomalies found, empty list if clean.
        """
        anomalies = []
        full_table = f"{self._schema}.{table}"

        try:
            anomalies.extend(self._check_row_count(full_table, table))
            anomalies.extend(self._check_nulls(full_table, table, columns))
            anomalies.extend(self._check_duplicates(full_table, table, columns[:1]))
            anomalies.extend(self._check_freshness(full_table, table))
        except Exception:
            logger.exception("Redshift check failed for table %s", full_table)

        return anomalies

    def _check_row_count(self, full_table: str, table: str) -> list[QualityAnomaly]:
        """Compare today's row count against 7-day average using date_trunc."""
        query = f"""
            WITH current_count AS (
                SELECT COUNT(*) AS row_count
                FROM {full_table}
                WHERE created_at >= CURRENT_DATE
            ),
            baseline AS (
                SELECT AVG(daily_count) AS avg_count
                FROM (
                    SELECT DATE_TRUNC('day', created_at) AS day,
                           COUNT(*) AS daily_count
                    FROM {full_table}
                    WHERE created_at BETWEEN CURRENT_DATE - INTERVAL '8 days'
                                         AND CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY 1
                ) daily
            )
            SELECT c.row_count,
                   b.avg_count,
                   CASE WHEN b.avg_count > 0
                        THEN (c.row_count - b.avg_count) / b.avg_count * 100
                        ELSE 0 END AS deviation_pct
            FROM current_count c CROSS JOIN baseline b
        """
        rows = self._execute(query)
        anomalies = []

        if rows:
            row = rows[0]
            deviation = float(row["deviation_pct"] or 0.0)
            if deviation < -ALERT_THRESHOLDS["volume_drop_pct"]:
                anomalies.append(QualityAnomaly(
                    table=table,
                    column=None,
                    issue_type="volume_drop",
                    current_value=float(row["row_count"]),
                    baseline_value=float(row["avg_count"] or 0),
                    deviation_pct=deviation,
                    sample_rows=[],
                ))
            elif deviation > ALERT_THRESHOLDS["volume_surge_pct"]:
                anomalies.append(QualityAnomaly(
                    table=table,
                    column=None,
                    issue_type="volume_surge",
                    current_value=float(row["row_count"]),
                    baseline_value=float(row["avg_count"] or 0),
                    deviation_pct=deviation,
                    sample_rows=[],
                ))

        return anomalies

    def _check_nulls(self, full_table: str, table: str, columns: list[str]) -> list[QualityAnomaly]:
        """Check null rate vs 7-day baseline per column."""
        anomalies = []

        for column in columns:
            query = f"""
                WITH today AS (
                    SELECT SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)::FLOAT
                           / NULLIF(COUNT(*), 0) * 100 AS null_pct
                    FROM {full_table}
                    WHERE created_at >= CURRENT_DATE
                ),
                baseline AS (
                    SELECT AVG(null_pct) AS avg_null_pct
                    FROM (
                        SELECT DATE_TRUNC('day', created_at) AS day,
                               SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)::FLOAT
                               / NULLIF(COUNT(*), 0) * 100 AS null_pct
                        FROM {full_table}
                        WHERE created_at BETWEEN CURRENT_DATE - INTERVAL '8 days'
                                             AND CURRENT_DATE - INTERVAL '1 day'
                        GROUP BY 1
                    ) daily
                )
                SELECT t.null_pct,
                       b.avg_null_pct,
                       t.null_pct - COALESCE(b.avg_null_pct, 0) AS spike_pct
                FROM today t CROSS JOIN baseline b
            """
            rows = self._execute(query)
            if rows:
                row = rows[0]
                spike = float(row["spike_pct"] or 0.0)
                if spike > ALERT_THRESHOLDS["null_spike_pct"]:
                    anomalies.append(QualityAnomaly(
                        table=table,
                        column=column,
                        issue_type="null_spike",
                        current_value=float(row["null_pct"] or 0),
                        baseline_value=float(row["avg_null_pct"] or 0),
                        deviation_pct=spike,
                        sample_rows=[],
                    ))

        return anomalies

    def _check_duplicates(self, full_table: str, table: str, key_columns: list[str]) -> list[QualityAnomaly]:
        """Check for duplicate primary keys in today's data."""
        if not key_columns:
            return []

        key_expr = ", ".join(key_columns)
        query = f"""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT {key_expr}) AS unique_rows,
                   (COUNT(*) - COUNT(DISTINCT {key_expr}))::FLOAT
                   / NULLIF(COUNT(*), 0) * 100 AS duplicate_pct
            FROM {full_table}
            WHERE created_at >= CURRENT_DATE
        """
        rows = self._execute(query)
        anomalies = []

        if rows and float(rows[0]["duplicate_pct"] or 0) > ALERT_THRESHOLDS["duplicate_rate_pct"]:
            row = rows[0]
            anomalies.append(QualityAnomaly(
                table=table,
                column=key_expr,
                issue_type="duplicates",
                current_value=float(row["duplicate_pct"]),
                baseline_value=0.0,
                deviation_pct=float(row["duplicate_pct"]),
                sample_rows=[],
            ))

        return anomalies

    def _check_freshness(self, full_table: str, table: str) -> list[QualityAnomaly]:
        """Alert if last record is older than freshness threshold."""
        query = f"""
            SELECT DATEDIFF(HOUR, MAX(created_at), SYSDATE) AS hours_since_update
            FROM {full_table}
        """
        rows = self._execute(query)
        anomalies = []

        if rows and rows[0]["hours_since_update"] is not None:
            hours_stale = float(rows[0]["hours_since_update"])
            if hours_stale > ALERT_THRESHOLDS["freshness_hours"]:
                anomalies.append(QualityAnomaly(
                    table=table,
                    column=None,
                    issue_type="freshness",
                    current_value=hours_stale,
                    baseline_value=ALERT_THRESHOLDS["freshness_hours"],
                    deviation_pct=hours_stale - ALERT_THRESHOLDS["freshness_hours"],
                    sample_rows=[],
                ))

        return anomalies

    def list_schema(self, table: str) -> list[dict]:
        """Return column names and types for schema drift detection."""
        query = """
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        return self._execute(query, (self._schema, table))
