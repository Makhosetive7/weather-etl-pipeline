"""Integration tests: mocked API + real PostgreSQL."""

from unittest.mock import patch

import pytest
from sqlalchemy import text

from main import EXIT_SUCCESS, run_etl_pipeline


@pytest.mark.integration
class TestPipelineIntegration:

    @patch("main.Config.CITIES", [{"name": "London", "country": "GB"}])
    @patch("src.extract.requests.get")
    def test_pipeline_loads_measurement(self, mock_get, mock_requests_response, integration_engine):
        mock_get.return_value = mock_requests_response

        with patch("main.Config.validate", return_value=True):
            exit_code = run_etl_pipeline()

        assert exit_code == EXIT_SUCCESS

        with integration_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM weather.weather_measurements")).scalar()
            runs = conn.execute(
                text(
                    "SELECT status, records_loaded FROM weather.etl_runs "
                    "ORDER BY started_at DESC LIMIT 1"
                )
            ).fetchone()

        assert count >= 1
        assert runs is not None
        assert runs[0] in ("success", "partial")
        assert runs[1] >= 1

    @patch("main.Config.CITIES", [{"name": "London", "country": "GB"}])
    @patch("src.extract.requests.get")
    def test_pipeline_idempotent_rerun(self, mock_get, mock_requests_response, integration_engine):
        mock_get.return_value = mock_requests_response

        with patch("main.Config.validate", return_value=True):
            run_etl_pipeline()
            exit_code = run_etl_pipeline()

        assert exit_code == EXIT_SUCCESS

        with integration_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM weather.weather_measurements")).scalar()

        assert count >= 1
