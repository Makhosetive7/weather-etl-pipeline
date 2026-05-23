"""Unit tests for the FastAPI read API."""

from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture
def client():
    return TestClient(app)


SAMPLE_LATEST = [
    {
        "city_id": 1,
        "city_name": "London",
        "country_code": "GB",
        "temperature_celsius": 15.5,
        "feels_like_celsius": 14.2,
        "humidity_percent": 65,
        "wind_speed_mps": 3.5,
        "main_condition": "Clear",
        "description": "clear sky",
        "measurement_timestamp": datetime(2021, 1, 1, 0, 0, 0),
    },
    {
        "city_id": 2,
        "city_name": "Paris",
        "country_code": "FR",
        "temperature_celsius": 5.0,
        "feels_like_celsius": 4.0,
        "humidity_percent": 70,
        "wind_speed_mps": 2.0,
        "main_condition": "Clouds",
        "description": "few clouds",
        "measurement_timestamp": datetime(2021, 1, 1, 0, 0, 0),
    },
]


class TestHealth:

    @patch("api.main.db.test_connection", return_value=True)
    def test_health_ok(self, mock_db, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "database": "connected"}

    @patch("api.main.db.test_connection", return_value=False)
    def test_health_degraded(self, mock_db, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "degraded"


class TestLatestWeather:

    @patch("api.main.db.fetch_latest_weather", return_value=SAMPLE_LATEST)
    def test_get_latest_weather(self, mock_fetch, client):
        response = client.get("/api/v1/weather/latest")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["city_name"] == "London"


class TestCities:

    @patch(
        "api.main.db.fetch_cities",
        return_value=[
            {
                "city_id": 1,
                "city_name": "London",
                "country_code": "GB",
                "latitude": 51.5,
                "longitude": -0.12,
            }
        ],
    )
    def test_list_cities(self, mock_fetch, client):
        response = client.get("/api/v1/cities")
        assert response.status_code == 200
        assert response.json()[0]["city_name"] == "London"

    @patch("api.main.db.fetch_city_by_id", return_value=None)
    def test_measurements_city_not_found(self, mock_city, client):
        response = client.get("/api/v1/cities/999/measurements")
        assert response.status_code == 404

    @patch("api.main.db.fetch_city_by_id")
    @patch("api.main.db.fetch_measurements")
    def test_get_city_measurements(self, mock_measurements, mock_city, client):
        mock_city.return_value = {"city_id": 1, "city_name": "London", "country_code": "GB"}
        mock_measurements.return_value = [
            {
                "measurement_id": 10,
                "city_id": 1,
                "temperature_celsius": 15.5,
                "feels_like_celsius": 14.2,
                "humidity_percent": 65,
                "wind_speed_mps": 3.5,
                "main_condition": "Clear",
                "description": "clear sky",
                "measurement_timestamp": "2021-01-01T00:00:00",
            }
        ]

        response = client.get("/api/v1/cities/1/measurements?limit=10")
        assert response.status_code == 200
        body = response.json()
        assert body["city_id"] == 1
        assert body["count"] == 1
        assert len(body["items"]) == 1


class TestAnalytics:

    @patch(
        "api.main.db.fetch_temperature_summary",
        return_value=[
            {
                "city_name": "London",
                "country_code": "GB",
                "avg_temp_c": 12.5,
                "measurement_count": 24,
            }
        ],
    )
    def test_temperature_summary(self, mock_summary, client):
        response = client.get("/api/v1/analytics/temperature-summary?days=7")
        assert response.status_code == 200
        assert response.json()[0]["avg_temp_c"] == 12.5

    @patch("api.main.db.fetch_latest_weather", return_value=SAMPLE_LATEST)
    def test_weather_extremes(self, mock_latest, client):
        response = client.get("/api/v1/analytics/extremes")
        assert response.status_code == 200
        body = response.json()
        assert body["hottest"]["city_name"] == "London"
        assert body["coldest"]["city_name"] == "Paris"

    @patch("api.main.db.fetch_latest_weather", return_value=[])
    def test_weather_extremes_empty(self, mock_latest, client):
        response = client.get("/api/v1/analytics/extremes")
        assert response.status_code == 200
        assert response.json() == {"hottest": None, "coldest": None}

    @patch("api.main.db.fetch_recent_etl_runs")
    def test_etl_runs(self, mock_runs, client):
        run_id = uuid4()
        mock_runs.return_value = [
            {
                "run_id": run_id,
                "status": "success",
                "records_extracted": 10,
                "records_transformed": 10,
                "records_loaded": 10,
                "records_failed": 0,
                "started_at": datetime(2021, 1, 1, 12, 0, 0),
                "finished_at": datetime(2021, 1, 1, 12, 1, 0),
                "duration_seconds": 60.0,
            }
        ]

        response = client.get("/api/v1/analytics/etl-runs")
        assert response.status_code == 200
        assert response.json()[0]["status"] == "success"


class TestOpenAPI:

    def test_openapi_schema_available(self, client):
        response = client.get("/openapi.json")
        assert response.status_code == 200
        assert response.json()["info"]["title"] == "Weather Analytics API"

    def test_docs_available(self, client):
        response = client.get("/docs")
        assert response.status_code == 200
