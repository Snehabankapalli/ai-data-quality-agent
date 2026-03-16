"""
Anomaly detection — statistical z-score analysis on pipeline metrics.
Flags sudden drops or spikes in row counts, averages, and sums.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
import structlog
from scipy import stats

from src.snowflake_client import SnowflakeClient

logger = structlog.get_logger()


@dataclass
class AnomalyResult:
    """Result of a single anomaly detection check."""

    table: str
    metric: str
    current_value: float
    mean_value: float
    std_value: float
    z_score: float
    z_score_threshold: float
    passed: bool
    severity: str
    message: str


class AnomalyChecker:
    """
    Z-score based anomaly detection for pipeline metrics.

    Uses a rolling window of historical values to establish a baseline,
    then flags current values that deviate beyond the configured threshold.
    """

    def __init__(self, snowflake_client: SnowflakeClient):
        self._sf = snowflake_client

    def run(self, check_config: dict[str, Any]) -> AnomalyResult:
        """
        Run anomaly detection on a table metric.

        Args:
            check_config: Dict with: table, metric, window_days,
                          z_score_threshold, severity, min_expected_rows (optional)

        Returns:
            AnomalyResult with z-score and pass/fail
        """
        table = check_config["table"]
        metric = check_config["metric"]
        window_days = check_config.get("window_days", 30)
        threshold = check_config.get("z_score_threshold", 3.0)
        severity = check_config.get("severity", "medium")
        min_rows = check_config.get("min_expected_rows", 0)

        logger.info(
            "running_anomaly_check",
            table=table,
            metric=metric,
            window_days=window_days,
        )

        try:
            history_df = self._sf.get_row_count_history(table, window_days)
        except Exception as e:
            logger.error("anomaly_check_error", table=table, error=str(e))
            return AnomalyResult(
                table=table,
                metric=metric,
                current_value=0,
                mean_value=0,
                std_value=0,
                z_score=0,
                z_score_threshold=threshold,
                passed=False,
                severity="critical",
                message=f"Failed to retrieve history: {e}",
            )

        if history_df.empty or len(history_df) < 3:
            return AnomalyResult(
                table=table,
                metric=metric,
                current_value=0,
                mean_value=0,
                std_value=0,
                z_score=0,
                z_score_threshold=threshold,
                passed=True,
                severity="ok",
                message="Insufficient history for anomaly detection (< 3 data points).",
            )

        values = history_df["ROW_COUNT"].astype(float).values
        current_value = float(values[-1])
        historical_values = values[:-1]

        mean_val = float(np.mean(historical_values))
        std_val = float(np.std(historical_values, ddof=1))

        if std_val == 0:
            z_score = 0.0
        else:
            z_score = float(abs((current_value - mean_val) / std_val))

        # Check min row count separately
        below_minimum = min_rows > 0 and current_value < min_rows

        passed = z_score <= threshold and not below_minimum

        if not passed:
            if below_minimum:
                msg = (
                    f"{table}.{metric} = {current_value:,.0f} is below "
                    f"minimum expected {min_rows:,} rows."
                )
            else:
                direction = "dropped" if current_value < mean_val else "spiked"
                msg = (
                    f"{table}.{metric} {direction} to {current_value:,.0f} "
                    f"(z={z_score:.2f}, threshold={threshold}, "
                    f"30d mean={mean_val:,.0f})"
                )
        else:
            msg = (
                f"{table}.{metric} = {current_value:,.0f} is normal "
                f"(z={z_score:.2f} ≤ {threshold})"
            )

        logger.info(
            "anomaly_check_complete",
            table=table,
            metric=metric,
            z_score=round(z_score, 3),
            passed=passed,
        )

        return AnomalyResult(
            table=table,
            metric=metric,
            current_value=current_value,
            mean_value=mean_val,
            std_value=std_val,
            z_score=z_score,
            z_score_threshold=threshold,
            passed=passed,
            severity=severity if not passed else "ok",
            message=msg,
        )

    def run_all(self, checks: list[dict[str, Any]]) -> list[AnomalyResult]:
        """Run all configured anomaly checks."""
        return [self.run(check) for check in checks]
