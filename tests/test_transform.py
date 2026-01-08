import pytest
from datetime import datetime
from src.transform import WeatherTransformer
from src.utils import validate_weather_data


class TestWeatherTransformer:
    
    def test_transformer_initialization(self):
        """Test that transformer initializes correctly"""
        transformer = WeatherTransformer()
        assert transformer is not None
    
    def test_transform_single_record_success(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(sample_raw_weather_data)
        
        assert result is not None
        assert 'city' in result
        assert 'condition' in result
        assert 'measurement' in result
    
    def test_transform_city_data(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(sample_raw_weather_data)
        
        city = result['city']
        assert city['city_name'] == 'London'
        assert city['country_code'] == 'GB'
        assert city['latitude'] == 51.5085
        assert city['longitude'] == -0.1257
        assert city['timezone_offset'] == 0
    
    def test_transform_condition_data(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(sample_raw_weather_data)
        
        condition = result['condition']
        assert condition['main_condition'] == 'Clear'
        assert condition['description'] == 'clear sky'
        assert condition['icon_code'] == '01d'
    
    def test_transform_measurement_data(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(sample_raw_weather_data)
        
        measurement = result['measurement']
        assert measurement['temperature_celsius'] == 15.5
        assert measurement['feels_like_celsius'] == 14.2
        assert measurement['temp_min_celsius'] == 13.0
        assert measurement['temp_max_celsius'] == 17.0
        assert measurement['pressure_hpa'] == 1013
        assert measurement['humidity_percent'] == 65
        assert measurement['visibility_meters'] == 10000
        assert measurement['wind_speed_mps'] == 3.5
        assert measurement['wind_direction_degrees'] == 180
        assert measurement['cloudiness_percent'] == 20
    
    def test_transform_timestamps(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(sample_raw_weather_data)
        
        measurement = result['measurement']
        assert 'measurement_timestamp' in measurement
        assert isinstance(measurement['measurement_timestamp'], datetime)
        assert 'api_call_timestamp' in measurement
        assert isinstance(measurement['api_call_timestamp'], datetime)
    
    def test_transform_missing_required_field(self, invalid_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(invalid_weather_data)
        
        assert result is None
    
    def test_transform_multiple_records_success(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        raw_data_list = [sample_raw_weather_data, sample_raw_weather_data.copy()]
        
        results = transformer.transform_multiple_records(raw_data_list)
        
        assert len(results) == 2
        assert all('city' in r for r in results)
        assert all('condition' in r for r in results)
        assert all('measurement' in r for r in results)
    
    def test_transform_multiple_records_partial_failure(self, sample_raw_weather_data, invalid_weather_data):
        transformer = WeatherTransformer()
        raw_data_list = [
            sample_raw_weather_data,
            invalid_weather_data,
            sample_raw_weather_data.copy()
        ]
        
        results = transformer.transform_multiple_records(raw_data_list)
        
        # Should get 2 successful transformations out of 3
        assert len(results) == 2
    
    def test_validate_transformed_data_success(self, sample_transformed_data):
        transformer = WeatherTransformer()
        result = transformer.validate_transformed_data(sample_transformed_data)
        
        assert result is True
    
    def test_validate_transformed_data_missing_section(self, sample_transformed_data):
        transformer = WeatherTransformer()
        del sample_transformed_data['city']
        
        result = transformer.validate_transformed_data(sample_transformed_data)
        
        assert result is False
    
    def test_validate_transformed_data_invalid_temperature(self, sample_transformed_data):
        transformer = WeatherTransformer()
        sample_transformed_data['measurement']['temperature_celsius'] = 150.0
        
        result = transformer.validate_transformed_data(sample_transformed_data)
        
        assert result is False
    
    def test_validate_transformed_data_invalid_humidity(self, sample_transformed_data):
        transformer = WeatherTransformer()
        sample_transformed_data['measurement']['humidity_percent'] = 150
        
        result = transformer.validate_transformed_data(sample_transformed_data)
        
        assert result is False
    
    def test_validate_transformed_data_negative_humidity(self, sample_transformed_data):
        transformer = WeatherTransformer()
        sample_transformed_data['measurement']['humidity_percent'] = -10
        
        result = transformer.validate_transformed_data(sample_transformed_data)
        
        assert result is False
    
    def test_transform_with_missing_optional_fields(self):
        transformer = WeatherTransformer()
        minimal_data = {
            'coord': {'lon': -0.1257, 'lat': 51.5085},
            'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}],
            'main': {
                'temp': 15.5,
                'feels_like': 14.2,
                'temp_min': 13.0,
                'temp_max': 17.0,
                'pressure': 1013,
                'humidity': 65
            },
            'dt': 1609459200,
            'sys': {'country': 'GB'},
            'name': 'London'
        }
        
        result = transformer.transform_single_record(minimal_data)
        
        assert result is not None
        assert result['measurement']['visibility_meters'] is None
        assert result['measurement']['wind_speed_mps'] is None
        assert result['measurement']['wind_direction_degrees'] is None
        assert result['measurement']['cloudiness_percent'] is None
    
    def test_transform_temperature_rounding(self):
        transformer = WeatherTransformer()
        data = {
            'coord': {'lon': 0, 'lat': 0},
            'weather': [{'main': 'Clear', 'description': 'clear', 'icon': '01d'}],
            'main': {
                'temp': 15.556,  # Changed to 15.556 to test rounding up
                'feels_like': 14.777,
                'temp_min': 13.333,
                'temp_max': 17.999,
                'pressure': 1013,
                'humidity': 65
            },
            'dt': 1609459200,
            'sys': {'country': 'GB'},
            'name': 'TestCity'
        }
        
        result = transformer.transform_single_record(data)
        
        assert result['measurement']['temperature_celsius'] == 15.56
        assert result['measurement']['feels_like_celsius'] == 14.78
        assert result['measurement']['temp_min_celsius'] == 13.33
        assert result['measurement']['temp_max_celsius'] == 18.0
    
    def test_transform_with_multiple_weather_conditions(self):
        transformer = WeatherTransformer()
        data = {
            'coord': {'lon': 0, 'lat': 0},
            'weather': [
                {'main': 'Rain', 'description': 'light rain', 'icon': '10d'},
                {'main': 'Clouds', 'description': 'few clouds', 'icon': '02d'}
            ],
            'main': {
                'temp': 15.5,
                'feels_like': 14.2,
                'temp_min': 13.0,
                'temp_max': 17.0,
                'pressure': 1013,
                'humidity': 65
            },
            'dt': 1609459200,
            'sys': {'country': 'GB'},
            'name': 'TestCity'
        }
        
        result = transformer.transform_single_record(data)
        
        # Should use the first weather condition
        assert result['condition']['main_condition'] == 'Rain'
        assert result['condition']['description'] == 'light rain'
    
    def test_validate_extreme_but_valid_temperatures(self, sample_transformed_data):
        transformer = WeatherTransformer()
        
        # Test very cold temperature (just within valid range)
        sample_transformed_data['measurement']['temperature_celsius'] = -99.0
        assert transformer.validate_transformed_data(sample_transformed_data) is True
        
        # Test very hot temperature (just within valid range)
        sample_transformed_data['measurement']['temperature_celsius'] = 59.0
        assert transformer.validate_transformed_data(sample_transformed_data) is True
    
    def test_transform_empty_list(self):
        """Test transformation of empty list"""
        transformer = WeatherTransformer()
        results = transformer.transform_multiple_records([])
        
        assert len(results) == 0
    
    def test_transform_preserves_data_types(self, sample_raw_weather_data):
        transformer = WeatherTransformer()
        result = transformer.transform_single_record(sample_raw_weather_data)
        
        measurement = result['measurement']
        assert isinstance(measurement['temperature_celsius'], float)
        assert isinstance(measurement['pressure_hpa'], int)
        assert isinstance(measurement['humidity_percent'], int)
        assert isinstance(measurement['measurement_timestamp'], datetime)