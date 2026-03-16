"""
Freshness checks — validates that tables are loaded within SLA windows.
Critical for same-day credit reporting pipelines (SoFi-grade).
"""

from dataclasses import dataclass
from typing import Any

import structlog

from src.snowflake_client import SnowflakeClient

logger = structlog.get_logger()


@dataclass
class FreshnessResult:
    """Result of a single freshness check."""

    table: str
    hours_since_load: float
    max_hours_allowed: float
    row_count: int
    last_load_time: str
    passed: bool
    severity: str
    message: str


class FreshnessChecker:
    """Validates data freshness against configured SLA windows."""

    def __init__(self, snowflake_client: SnowflakeClient):
        self._sf = snowflake_client

    def run(self, check_config: dict[str, Any]) -> FreshnessResult:
        """
        Run a single freshness check.

        Args:
            check_config: Dict with keys: table, max_hours_since_load, severity

        Returns:
            FreshnessResult with pass/fail status and context
        """
        table = check_config["table"]
        max_hours = check_config["max_hours_since_load"]
        severity = check_config.get("severity", "medium")

        logger.info("running_freshness_check", table=table, max_hours=max_hours)

        try:
            stats = self._sf.get_table_freshness(table)
        except Exception as e:
            logger.error("freshness_check_error", table=table, error=str(e))
            return FreshnessResult(
                table=table,
                hours_since_load=-1,
                max_hours_allowed=max_hours,
                row_count=0,
                last_load_time="UNKNOWN",
                passed=False,
                severity="critical",
                message=f"Failed to retrieve freshness data: {e}",
            )

        passed = stats["hours_since_load"] <= max_hours
        message = (
            f"Table {table} loaded {stats['hours_since_load']:.1f}h ago "
            f"(SLA: {max_hours}h, rows: {stats['row_count']:,})"
        )

        logger.info(
            "freshness_check_complete",
            table=table,
            passed=passed,
            hours_since_load=stats["hours_since_load"],
        )

        return FreshnessResult(
            table=table,
            hours_since_load=stats["hours_since_load"],
            max_hours_allowed=max_hours,
            row_count=stats["row_count"],
            last_load_time=stats["last_load_time"],
            passed=passed,
            severity=severity if not passed else "ok",
            message=message,
        )

    def run_all(self, checks: list[dict[str, Any]]) -> list[FreshnessResult]:
        """Run all configured freshness checks and return results."""
        return [self.run(check) for check in checks]
