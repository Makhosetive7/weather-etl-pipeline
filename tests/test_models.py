"""Unit tests for Pydantic models."""

import pytest
from pydantic import ValidationError

from src.models import (
    TransformedWeatherRecord,
    build_transformed_record,
    parse_openweather_response,
)


class TestOpenWeatherResponse:

    def test_parse_valid_response(self, sample_raw_weather_data):
        parsed = parse_openweather_response(sample_raw_weather_data)
        assert parsed.name == "London"
        assert parsed.main.temp == 15.5

    def test_rejects_missing_coord(self, invalid_weather_data):
        with pytest.raises(ValidationError):
            parse_openweather_response(invalid_weather_data)

    def test_rejects_extreme_temperature(self, sample_raw_weather_data):
        data = {**sample_raw_weather_data}
        data["main"] = {**data["main"], "temp": 150.0}
        with pytest.raises(ValidationError):
            parse_openweather_response(data)


class TestTransformedRecord:

    def test_build_transformed_record(self, sample_raw_weather_data):
        parsed = parse_openweather_response(sample_raw_weather_data)
        record = build_transformed_record(parsed)

        assert record.city.city_name == "London"
        assert record.measurement.temperature_celsius == 15.5
        assert record.condition.main_condition == "Clear"

    def test_to_dict_for_loader(self, sample_raw_weather_data):
        parsed = parse_openweather_response(sample_raw_weather_data)
        data = build_transformed_record(parsed).to_dict()

        assert "city" in data
        assert "condition" in data
        assert "measurement" in data

    def test_transformed_record_validation(self, sample_transformed_data):
        record = TransformedWeatherRecord.model_validate(sample_transformed_data)
        assert record.city.country_code == "GB"

    def test_invalid_humidity(self, sample_transformed_data):
        data = {**sample_transformed_data}
        data["measurement"] = {**data["measurement"], "humidity_percent": 150}
        with pytest.raises(ValidationError):
            TransformedWeatherRecord.model_validate(data)
