"""
Unit tests for data quality checkers.
Uses mock Snowflake data — no live credentials required.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.checks.anomaly import AnomalyChecker
from src.checks.completeness import CompletenessChecker
from src.checks.freshness import FreshnessChecker


@pytest.fixture
def mock_sf():
    """Mock SnowflakeClient."""
    return MagicMock()


class TestFreshnessChecker:
    def test_passes_when_within_sla(self, mock_sf):
        mock_sf.get_table_freshness.return_value = {
            "table": "TRANSACTIONS",
            "hours_since_load": 2.5,
            "row_count": 500000,
            "last_load_time": "2024-01-15 10:00:00",
        }
        checker = FreshnessChecker(mock_sf)
        result = checker.run({"table": "TRANSACTIONS", "max_hours_since_load": 4, "severity": "critical"})
        assert result.passed is True
        assert result.severity == "ok"

    def test_fails_when_outside_sla(self, mock_sf):
        mock_sf.get_table_freshness.return_value = {
            "table": "TRANSACTIONS",
            "hours_since_load": 6.0,
            "row_count": 500000,
            "last_load_time": "2024-01-15 06:00:00",
        }
        checker = FreshnessChecker(mock_sf)
        result = checker.run({"table": "TRANSACTIONS", "max_hours_since_load": 4, "severity": "critical"})
        assert result.passed is False
        assert result.severity == "critical"

    def test_handles_snowflake_error(self, mock_sf):
        mock_sf.get_table_freshness.side_effect = Exception("Connection timeout")
        checker = FreshnessChecker(mock_sf)
        result = checker.run({"table": "TRANSACTIONS", "max_hours_since_load": 4, "severity": "high"})
        assert result.passed is False
        assert "Connection timeout" in result.message


class TestCompletenessChecker:
    def test_passes_all_columns(self, mock_sf):
        mock_sf.get_null_percentages.return_value = {
            "TRANSACTION_ID": 0.0,
            "AMOUNT": 0.0,
        }
        checker = CompletenessChecker(mock_sf)
        result = checker.run({
            "table": "TRANSACTIONS",
            "columns": [
                {"name": "TRANSACTION_ID", "max_null_pct": 0.0, "severity": "critical"},
                {"name": "AMOUNT", "max_null_pct": 0.0, "severity": "critical"},
            ],
        })
        assert result.passed is True

    def test_fails_exceeding_null_threshold(self, mock_sf):
        mock_sf.get_null_percentages.return_value = {
            "TRANSACTION_ID": 0.0,
            "AMOUNT": 5.2,
        }
        checker = CompletenessChecker(mock_sf)
        result = checker.run({
            "table": "TRANSACTIONS",
            "columns": [
                {"name": "TRANSACTION_ID", "max_null_pct": 0.0, "severity": "critical"},
                {"name": "AMOUNT", "max_null_pct": 0.0, "severity": "critical"},
            ],
        })
        assert result.passed is False
        assert result.worst_severity == "critical"
        assert any(c.column == "AMOUNT" and not c.passed for c in result.column_results)


class TestAnomalyChecker:
    def _make_history_df(self, values: list[float]) -> pd.DataFrame:
        import numpy as np
        return pd.DataFrame({
            "LOAD_DATE": pd.date_range("2024-01-01", periods=len(values)),
            "ROW_COUNT": values,
        })

    def test_passes_normal_volume(self, mock_sf):
        normal = [100000] * 29 + [102000]  # Slight uptick, z < 1
        mock_sf.get_row_count_history.return_value = self._make_history_df(normal)
        checker = AnomalyChecker(mock_sf)
        result = checker.run({
            "table": "TRANSACTIONS",
            "metric": "row_count",
            "window_days": 30,
            "z_score_threshold": 3.0,
            "severity": "high",
        })
        assert result.passed is True

    def test_fails_on_dramatic_drop(self, mock_sf):
        # 29 days normal, then almost zero
        values = [100000] * 29 + [5000]
        mock_sf.get_row_count_history.return_value = self._make_history_df(values)
        checker = AnomalyChecker(mock_sf)
        result = checker.run({
            "table": "TRANSACTIONS",
            "metric": "row_count",
            "window_days": 30,
            "z_score_threshold": 3.0,
            "severity": "high",
        })
        assert result.passed is False
        assert result.z_score > 3.0

    def test_passes_with_insufficient_history(self, mock_sf):
        mock_sf.get_row_count_history.return_value = self._make_history_df([100000, 105000])
        checker = AnomalyChecker(mock_sf)
        result = checker.run({
            "table": "TRANSACTIONS",
            "metric": "row_count",
            "window_days": 30,
            "z_score_threshold": 3.0,
            "severity": "high",
        })
        assert result.passed is True  # Not enough data to flag
