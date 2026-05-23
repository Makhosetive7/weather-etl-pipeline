"""Unit tests for shared utilities."""

import logging
from unittest.mock import patch

import pytest

from src.utils import (
    kelvin_to_celsius,
    kelvin_to_fahrenheit,
    retry_on_failure,
    setup_logging,
    validate_weather_data,
)


class TestRetryDecorator:

    def test_succeeds_first_attempt(self):
        call_count = {"n": 0}

        @retry_on_failure(max_retries=3, delay=0)
        def succeeds():
            call_count["n"] += 1
            return "ok"

        assert succeeds() == "ok"
        assert call_count["n"] == 1

    def test_retries_then_succeeds(self):
        call_count = {"n": 0}

        @retry_on_failure(max_retries=3, delay=0)
        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise ConnectionError("timeout")
            return "ok"

        assert flaky() == "ok"
        assert call_count["n"] == 2

    def test_raises_after_max_retries(self):
        @retry_on_failure(max_retries=2, delay=0)
        def always_fails():
            raise RuntimeError("permanent failure")

        with pytest.raises(RuntimeError, match="permanent failure"):
            always_fails()


class TestTemperatureConversion:

    def test_kelvin_to_celsius(self):
        assert kelvin_to_celsius(273.15) == 0.0
        assert kelvin_to_celsius(None) is None

    def test_kelvin_to_fahrenheit(self):
        assert kelvin_to_fahrenheit(273.15) == 32.0
        assert kelvin_to_fahrenheit(None) is None


class TestValidateWeatherData:

    def test_valid_data(self, sample_raw_weather_data):
        assert validate_weather_data(sample_raw_weather_data) is True

    def test_missing_required_field(self, invalid_weather_data):
        with pytest.raises(ValueError, match="validation errors"):
            validate_weather_data(invalid_weather_data)

    def test_temperature_out_of_range(self, sample_raw_weather_data):
        data = {**sample_raw_weather_data}
        data["main"] = {**data["main"], "temp": 150.0}
        with pytest.raises(ValueError, match="Temperature out of reasonable range"):
            validate_weather_data(data)


class TestSetupLogging:

    def test_setup_logging_returns_logger(self, tmp_path):
        log_file = tmp_path / "logs" / "test.log"
        with patch("src.utils.Config.LOG_FILE", str(log_file)):
            with patch("src.utils.Config.LOG_LEVEL", "INFO"):
                with patch("src.utils.Config.LOG_FORMAT", "%(message)s"):
                    logger = setup_logging()
                    assert isinstance(logger, logging.Logger)
                    assert log_file.parent.exists()
