"""Tests for SnowflakeMonitor — mocks snowflake.connector."""

import pytest
from unittest.mock import MagicMock, patch, call

from src.monitors.snowflake_monitor import SnowflakeMonitor, QualityAnomaly, ALERT_THRESHOLDS


CONN_PARAMS = dict(
    account="acct123",
    user="test_user",
    password="test_pass",
    database="FINTECH_DB",
    schema="STAGING",
)


def _make_monitor():
    return SnowflakeMonitor(**CONN_PARAMS)


def _make_cursor(fetchone_return=None, fetchall_return=None):
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_return
    cursor.fetchall.return_value = fetchall_return or []
    return cursor


class TestSnowflakeMonitorVolumeCheck:
    def test_no_anomaly_when_within_threshold(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(10_000.0, 9_800.0, -2.0))  # -2% deviation
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("snowflake.connector.connect", return_value=mock_conn):
            anomalies = monitor._check_volume(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")

        assert anomalies == []

    def test_volume_drop_detected(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(10_000.0, 5_000.0, -50.0))  # -50% = drop
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_volume(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")

        assert len(anomalies) == 1
        assert anomalies[0].issue_type == "volume_drop"
        assert anomalies[0].deviation_pct == -50.0

    def test_volume_surge_detected(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(10_000.0, 35_000.0, 250.0))  # +250% = surge
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_volume(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")

        assert len(anomalies) == 1
        assert anomalies[0].issue_type == "volume_surge"

    def test_no_rows_returns_empty(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=None)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_volume(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")
        assert anomalies == []

    def test_exception_returns_empty(self):
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("connection error")

        anomalies = monitor._check_volume(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")
        assert anomalies == []


class TestSnowflakeMonitorNullCheck:
    def test_no_anomaly_when_within_threshold(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(1.0, 2.0))  # 1pp increase — below 5pp threshold
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_nulls(mock_conn, "STAGING.STG_CARD_TRANSACTIONS", "merchant_id")
        assert anomalies == []

    def test_null_spike_detected(self):
        monitor = _make_monitor()

        # First call: null check; second call: sample
        cursor_null = _make_cursor(fetchone_return=(1.0, 40.0))  # 39pp spike
        cursor_sample = _make_cursor(fetchall_return=[{"transaction_id": "x", "merchant_id": None}])

        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = [cursor_null, cursor_sample]

        anomalies = monitor._check_nulls(mock_conn, "STAGING.STG_CARD_TRANSACTIONS", "merchant_id")

        assert len(anomalies) == 1
        assert anomalies[0].issue_type == "null_spike"
        assert anomalies[0].column == "merchant_id"
        assert anomalies[0].deviation_pct == pytest.approx(39.0)

    def test_null_returns_empty_when_none_values(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(None, None))
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_nulls(mock_conn, "STAGING.STG_CARD_TRANSACTIONS", "merchant_id")
        assert anomalies == []

    def test_exception_returns_empty(self):
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("timeout")

        anomalies = monitor._check_nulls(mock_conn, "STAGING.STG_CARD_TRANSACTIONS", "col")
        assert anomalies == []


class TestSnowflakeMonitorDuplicateCheck:
    def test_no_anomaly_when_below_threshold(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(10_000, 10_000, 0.0))
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_duplicates(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")
        assert anomalies == []

    def test_duplicate_surge_detected(self):
        monitor = _make_monitor()
        cursor = _make_cursor(fetchone_return=(10_000, 9_800, 2.0))  # 2% dup rate > 1% threshold
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = cursor

        anomalies = monitor._check_duplicates(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")

        assert len(anomalies) == 1
        assert anomalies[0].issue_type == "duplicate_surge"
        assert anomalies[0].current_value == pytest.approx(2.0)

    def test_exception_returns_empty(self):
        monitor = _make_monitor()
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("query failed")

        anomalies = monitor._check_duplicates(mock_conn, "STAGING.STG_CARD_TRANSACTIONS")
        assert anomalies == []


class TestAlertThresholds:
    def test_null_spike_threshold_is_positive(self):
        assert ALERT_THRESHOLDS["null_spike_pct"] > 0

    def test_volume_drop_threshold_is_positive(self):
        assert ALERT_THRESHOLDS["volume_drop_pct"] > 0

    def test_duplicate_threshold_is_small(self):
        # Duplicate threshold should be tight — <5%
        assert ALERT_THRESHOLDS["duplicate_rate_pct"] < 5.0


class TestQualityAnomalyDataclass:
    def test_creates_with_all_fields(self):
        anomaly = QualityAnomaly(
            table="STAGING.STG_CARD_TRANSACTIONS",
            column="merchant_id",
            issue_type="null_spike",
            current_value=35.2,
            baseline_value=1.1,
            deviation_pct=34.1,
            sample_rows=[{"id": "1"}],
        )
        assert anomaly.table == "STAGING.STG_CARD_TRANSACTIONS"
        assert anomaly.deviation_pct == 34.1

    def test_column_can_be_none(self):
        anomaly = QualityAnomaly(
            table="T",
            column=None,
            issue_type="volume_drop",
            current_value=0.0,
            baseline_value=100.0,
            deviation_pct=-100.0,
            sample_rows=[],
        )
        assert anomaly.column is None
