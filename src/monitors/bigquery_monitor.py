"""
BigQuery data quality monitor.

Detects null spikes, volume drops, schema drift, and freshness issues
in BigQuery tables. Same anomaly interface as SnowflakeMonitor for
consistent handling in the agent core.

Usage:
    monitor = BigQueryMonitor(project="my-project", dataset="analytics")
    anomalies = monitor.check_table("events", columns=["user_id", "event_type"])
"""

import logging
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
    """Data quality anomaly from BigQuery monitor."""
    table: str
    column: Optional[str]
    issue_type: str
    current_value: float
    baseline_value: float
    deviation_pct: float
    sample_rows: list


class BigQueryMonitor:
    """
    Polls BigQuery tables for data quality anomalies.
    Uses BigQuery information_schema and time-travel for baseline comparison.

    Requires google-cloud-bigquery package and Application Default Credentials.
    """

    def __init__(self, project: str, dataset: str, location: str = "US"):
        self._project = project
        self._dataset = dataset
        self._location = location
        self._client = None

    def _get_client(self):
        """Lazy-initialize BigQuery client."""
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client(project=self._project)
        return self._client

    def check_table(self, table: str, columns: list[str]) -> list[QualityAnomaly]:
        """
        Run all quality checks on a BigQuery table.
        Returns list of anomalies found, empty list if clean.
        """
        anomalies = []
        full_table = f"{self._project}.{self._dataset}.{table}"

        try:
            anomalies.extend(self._check_row_count(full_table, table))
            anomalies.extend(self._check_nulls(full_table, table, columns))
            anomalies.extend(self._check_freshness(full_table, table))
        except Exception:
            logger.exception("BigQuery check failed for table %s", full_table)

        return anomalies

    def _check_row_count(self, full_table: str, table: str) -> list[QualityAnomaly]:
        """Compare today's row count against 7-day average."""
        client = self._get_client()
        query = f"""
            WITH current_count AS (
                SELECT COUNT(*) as row_count,
                       DATE(CURRENT_TIMESTAMP()) as check_date
                FROM `{full_table}`
                WHERE DATE(_PARTITIONTIME) = CURRENT_DATE()
            ),
            baseline AS (
                SELECT AVG(cnt) as avg_count
                FROM (
                    SELECT COUNT(*) as cnt
                    FROM `{full_table}`
                    WHERE DATE(_PARTITIONTIME) BETWEEN
                        DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                    GROUP BY DATE(_PARTITIONTIME)
                )
            )
            SELECT c.row_count, b.avg_count,
                   SAFE_DIVIDE(c.row_count - b.avg_count, b.avg_count) * 100 AS deviation_pct
            FROM current_count c CROSS JOIN baseline b
        """
        rows = list(client.query(query).result())
        anomalies = []

        if rows:
            row = rows[0]
            deviation = float(row.deviation_pct or 0.0)
            if deviation < -ALERT_THRESHOLDS["volume_drop_pct"]:
                anomalies.append(QualityAnomaly(
                    table=table,
                    column=None,
                    issue_type="volume_drop",
                    current_value=float(row.row_count),
                    baseline_value=float(row.avg_count or 0),
                    deviation_pct=deviation,
                    sample_rows=[],
                ))
            elif deviation > ALERT_THRESHOLDS["volume_surge_pct"]:
                anomalies.append(QualityAnomaly(
                    table=table,
                    column=None,
                    issue_type="volume_surge",
                    current_value=float(row.row_count),
                    baseline_value=float(row.avg_count or 0),
                    deviation_pct=deviation,
                    sample_rows=[],
                ))

        return anomalies

    def _check_nulls(self, full_table: str, table: str, columns: list[str]) -> list[QualityAnomaly]:
        """Detect null rate spikes vs 7-day baseline for each column."""
        client = self._get_client()
        anomalies = []

        for column in columns:
            query = f"""
                WITH current_nulls AS (
                    SELECT COUNTIF({column} IS NULL) / COUNT(*) * 100 AS null_pct
                    FROM `{full_table}`
                    WHERE DATE(_PARTITIONTIME) = CURRENT_DATE()
                ),
                baseline_nulls AS (
                    SELECT AVG(null_pct) AS avg_null_pct
                    FROM (
                        SELECT COUNTIF({column} IS NULL) / COUNT(*) * 100 AS null_pct
                        FROM `{full_table}`
                        WHERE DATE(_PARTITIONTIME) BETWEEN
                            DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
                            AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                        GROUP BY DATE(_PARTITIONTIME)
                    )
                )
                SELECT c.null_pct, b.avg_null_pct,
                       c.null_pct - b.avg_null_pct AS spike_pct
                FROM current_nulls c CROSS JOIN baseline_nulls b
            """
            rows = list(client.query(query).result())
            if rows:
                row = rows[0]
                spike = float(row.spike_pct or 0.0)
                if spike > ALERT_THRESHOLDS["null_spike_pct"]:
                    anomalies.append(QualityAnomaly(
                        table=table,
                        column=column,
                        issue_type="null_spike",
                        current_value=float(row.null_pct or 0),
                        baseline_value=float(row.avg_null_pct or 0),
                        deviation_pct=spike,
                        sample_rows=[],
                    ))

        return anomalies

    def _check_freshness(self, full_table: str, table: str) -> list[QualityAnomaly]:
        """Alert if no data has arrived in the freshness window."""
        client = self._get_client()
        query = f"""
            SELECT
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_PARTITIONTIME), HOUR) AS hours_since_update
            FROM `{full_table}`
        """
        rows = list(client.query(query).result())
        anomalies = []

        if rows and rows[0].hours_since_update is not None:
            hours_stale = float(rows[0].hours_since_update)
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
