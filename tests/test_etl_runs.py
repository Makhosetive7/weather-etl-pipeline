"""Unit tests for ETL run metadata tracking."""

from unittest.mock import MagicMock, patch

from src.etl_runs import EtlRunTracker


def _mock_conn():
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


class TestEtlRunTracker:

    @patch("src.etl_runs.create_engine")
    def test_start_run(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn()

        tracker = EtlRunTracker()
        run_id = tracker.start_run("test-run-id")

        assert run_id == "test-run-id"
        mock_engine.connect.return_value.execute.assert_called_once()
        mock_engine.connect.return_value.commit.assert_called_once()

    @patch("src.etl_runs.create_engine")
    def test_finish_run(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn()

        tracker = EtlRunTracker()
        tracker.finish_run(
            "test-run-id",
            "success",
            records_extracted=10,
            records_transformed=10,
            records_loaded=10,
        )

        mock_engine.connect.return_value.execute.assert_called_once()
        mock_engine.connect.return_value.commit.assert_called_once()
