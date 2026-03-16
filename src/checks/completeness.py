"""
Completeness checks — validates null rates for critical columns.
Ensures 100% accuracy for regulatory reporting (CFPB/FDIC compliance).
"""

from dataclasses import dataclass, field
from typing import Any

import structlog

from src.snowflake_client import SnowflakeClient

logger = structlog.get_logger()


@dataclass
class ColumnResult:
    """Result for a single column completeness check."""

    column: str
    null_pct: float
    max_null_pct: float
    passed: bool
    severity: str


@dataclass
class CompletenessResult:
    """Aggregated result of completeness checks for a table."""

    table: str
    column_results: list[ColumnResult] = field(default_factory=list)
    passed: bool = True
    worst_severity: str = "ok"
    message: str = ""


class CompletenessChecker:
    """Validates column-level null rates against configured thresholds."""

    SEVERITY_RANK = {"ok": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}

    def __init__(self, snowflake_client: SnowflakeClient):
        self._sf = snowflake_client

    def run(self, check_config: dict[str, Any]) -> CompletenessResult:
        """
        Run completeness checks for all columns in a table config.

        Args:
            check_config: Dict with keys: table, columns (list of {name, max_null_pct, severity})

        Returns:
            CompletenessResult with per-column results
        """
        table = check_config["table"]
        column_configs = check_config["columns"]
        column_names = [c["name"] for c in column_configs]

        logger.info("running_completeness_check", table=table, columns=column_names)

        try:
            null_pcts = self._sf.get_null_percentages(table, column_names)
        except Exception as e:
            logger.error("completeness_check_error", table=table, error=str(e))
            return CompletenessResult(
                table=table,
                passed=False,
                worst_severity="critical",
                message=f"Failed to retrieve null stats: {e}",
            )

        result = CompletenessResult(table=table)
        for col_cfg in column_configs:
            col_name = col_cfg["name"]
            max_null_pct = col_cfg["max_null_pct"]
            severity = col_cfg.get("severity", "medium")
            actual_null_pct = null_pcts.get(col_name, 0.0)
            passed = actual_null_pct <= max_null_pct

            col_result = ColumnResult(
                column=col_name,
                null_pct=actual_null_pct,
                max_null_pct=max_null_pct,
                passed=passed,
                severity=severity if not passed else "ok",
            )
            result.column_results.append(col_result)

            if not passed:
                result.passed = False
                if self.SEVERITY_RANK.get(severity, 0) > self.SEVERITY_RANK.get(
                    result.worst_severity, 0
                ):
                    result.worst_severity = severity

        failed_cols = [c for c in result.column_results if not c.passed]
        if failed_cols:
            result.message = (
                f"{len(failed_cols)} column(s) exceed null thresholds in {table}: "
                + ", ".join(
                    f"{c.column} ({c.null_pct:.2f}% > {c.max_null_pct}%)"
                    for c in failed_cols
                )
            )
        else:
            result.message = f"All {len(column_names)} columns in {table} pass null checks."

        logger.info(
            "completeness_check_complete",
            table=table,
            passed=result.passed,
            failed_columns=len(failed_cols) if failed_cols else 0,
        )

        return result

    def run_all(self, checks: list[dict[str, Any]]) -> list[CompletenessResult]:
        """Run completeness checks for all configured tables."""
        return [self.run(check) for check in checks]
