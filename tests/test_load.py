"""Unit tests for the weather data load module."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.load import WeatherLoader


def _make_loader(mock_engine):
    with patch("src.load.create_engine", return_value=mock_engine):
        return WeatherLoader()


def _mock_conn_with_fetchone(fetchone_value):
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = fetchone_value
    mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


def _mock_conn_with_execute_sequence(fetchone_values):
    """One connection, multiple execute() calls (select then insert)."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    results = []
    for value in fetchone_values:
        mock_result = MagicMock()
        mock_result.fetchone.return_value = value
        results.append(mock_result)

    mock_conn.execute.side_effect = results
    return mock_conn


class TestWeatherLoaderInit:

    @patch("src.load.create_engine")
    def test_loader_initialization(self, mock_create_engine, mock_engine):
        mock_create_engine.return_value = mock_engine
        loader = WeatherLoader()
        mock_create_engine.assert_called_once()
        assert loader.engine is mock_engine

    @patch("src.load.create_engine", side_effect=Exception("connection refused"))
    def test_loader_initialization_failure(self, mock_create_engine):
        with pytest.raises(Exception, match="connection refused"):
            WeatherLoader()


class TestLoadCity:

    @patch("src.load.create_engine")
    def test_load_city_existing(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn_with_fetchone((7,))

        loader = WeatherLoader()
        city_id = loader.load_city(sample_transformed_data["city"])

        assert city_id == 7
        mock_engine.connect.return_value.execute.assert_called_once()

    @patch("src.load.create_engine")
    def test_load_city_insert_new(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn_with_execute_sequence([None, (99,)])

        loader = WeatherLoader()
        city_id = loader.load_city(sample_transformed_data["city"])

        assert city_id == 99
        mock_engine.connect.return_value.commit.assert_called_once()

    @patch("src.load.create_engine")
    def test_load_city_database_error(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("db error")
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine.connect.return_value = mock_conn

        loader = WeatherLoader()
        assert loader.load_city(sample_transformed_data["city"]) is None


class TestLoadWeatherCondition:

    @patch("src.load.create_engine")
    def test_load_condition_existing(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn_with_fetchone((3,))

        loader = WeatherLoader()
        condition_id = loader.load_weather_condition(sample_transformed_data["condition"])

        assert condition_id == 3

    @patch("src.load.create_engine")
    def test_load_condition_insert_new(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn_with_execute_sequence([None, (12,)])

        loader = WeatherLoader()
        condition_id = loader.load_weather_condition(sample_transformed_data["condition"])

        assert condition_id == 12
        mock_engine.connect.return_value.commit.assert_called_once()


class TestLoadMeasurement:

    @patch("src.load.create_engine")
    def test_load_measurement_success(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn_with_fetchone(None)

        loader = WeatherLoader()
        success = loader.load_measurement(
            sample_transformed_data["measurement"], city_id=1, condition_id=2
        )

        assert success is True
        mock_engine.connect.return_value.commit.assert_called_once()

    @patch("src.load.create_engine")
    def test_load_measurement_strips_api_call_timestamp(
        self, mock_create_engine, sample_transformed_data
    ):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = _mock_conn_with_fetchone(None)
        mock_engine.connect.return_value = mock_conn

        loader = WeatherLoader()
        loader.load_measurement(sample_transformed_data["measurement"], city_id=1, condition_id=2)

        call_args = mock_conn.execute.call_args
        assert "api_call_timestamp" not in call_args[0][1]

    @patch("src.load.create_engine")
    def test_load_measurement_database_error(self, mock_create_engine, sample_transformed_data):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("insert failed")
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine.connect.return_value = mock_conn

        loader = WeatherLoader()
        success = loader.load_measurement(
            sample_transformed_data["measurement"], city_id=1, condition_id=2
        )

        assert success is False


class TestLoadWeatherRecord:

    @patch.object(WeatherLoader, "load_measurement", return_value=True)
    @patch.object(WeatherLoader, "load_weather_condition", return_value=2)
    @patch.object(WeatherLoader, "load_city", return_value=1)
    @patch("src.load.create_engine")
    def test_load_weather_record_success(
        self,
        mock_create_engine,
        mock_load_city,
        mock_load_condition,
        mock_load_measurement,
        sample_transformed_data,
    ):
        mock_create_engine.return_value = MagicMock()
        loader = WeatherLoader()
        assert loader.load_weather_record(sample_transformed_data) is True

    @patch.object(WeatherLoader, "load_city", return_value=None)
    @patch("src.load.create_engine")
    def test_load_weather_record_city_failure(self, mock_create_engine, sample_transformed_data):
        mock_create_engine.return_value = MagicMock()
        loader = WeatherLoader()
        assert loader.load_weather_record(sample_transformed_data) is False

    @patch.object(WeatherLoader, "load_measurement", return_value=False)
    @patch.object(WeatherLoader, "load_weather_condition", return_value=2)
    @patch.object(WeatherLoader, "load_city", return_value=1)
    @patch("src.load.create_engine")
    def test_load_weather_record_measurement_failure(
        self,
        mock_create_engine,
        mock_load_city,
        mock_load_condition,
        mock_load_measurement,
        sample_transformed_data,
    ):
        mock_create_engine.return_value = MagicMock()
        loader = WeatherLoader()
        assert loader.load_weather_record(sample_transformed_data) is False


class TestLoadMultipleRecords:

    @patch.object(WeatherLoader, "load_weather_record")
    @patch("src.load.create_engine")
    def test_load_multiple_records_counts(
        self, mock_create_engine, mock_load_record, sample_transformed_data
    ):
        mock_create_engine.return_value = MagicMock()
        mock_load_record.side_effect = [True, False, True]

        loader = WeatherLoader()
        result = loader.load_multiple_records(
            [sample_transformed_data, sample_transformed_data, sample_transformed_data]
        )

        assert result == {"success": 2, "failure": 1, "total": 3}


class TestLoaderConnection:

    @patch("src.load.create_engine")
    def test_connection_success(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = _mock_conn_with_fetchone((1,))

        loader = WeatherLoader()
        assert loader.test_connection() is True

    @patch("src.load.create_engine")
    def test_connection_failure(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.side_effect = Exception("connection refused")

        loader = WeatherLoader()
        assert loader.test_connection() is False

    @patch("src.load.create_engine")
    def test_close_disposes_engine(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        loader = WeatherLoader()
        loader.close()

        mock_engine.dispose.assert_called_once()
