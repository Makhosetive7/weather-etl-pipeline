"""
Unit tests for the weather data extraction module
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from src.extract import WeatherExtractor
from config.config import Config


class TestWeatherExtractor:
    
    def test_extractor_initialization(self):
        extractor = WeatherExtractor()
        assert extractor.api_key == Config.API_KEY
        assert extractor.base_url == Config.API_BASE_URL
        assert extractor.timeout == Config.REQUEST_TIMEOUT
    
    def test_extractor_initialization_without_api_key(self):
        with patch('config.config.Config.API_KEY', None):
            with pytest.raises(ValueError, match="API key not configured"):
                WeatherExtractor()
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_by_city_success(self, mock_get, mock_requests_response):
        mock_get.return_value = mock_requests_response
        
        extractor = WeatherExtractor()
        result = extractor.fetch_weather_by_city('London', 'GB')
        
        assert result is not None
        assert result['name'] == 'London'
        assert result['sys']['country'] == 'GB'
        assert 'main' in result
        assert 'temp' in result['main']
        
        # Verify API call was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert 'params' in call_args.kwargs
        assert call_args.kwargs['params']['q'] == 'London,GB'
        assert call_args.kwargs['params']['units'] == 'metric'
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_by_city_without_country(self, mock_get, mock_requests_response):
        """Test weather fetch without country code"""
        mock_get.return_value = mock_requests_response
        
        extractor = WeatherExtractor()
        result = extractor.fetch_weather_by_city('London')
        
        assert result is not None
        call_args = mock_get.call_args
        assert call_args.kwargs['params']['q'] == 'London'
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_http_401_error(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        extractor = WeatherExtractor()
        result = extractor.fetch_weather_by_city('London', 'GB')
        
        assert result is None
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_http_404_error(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        extractor = WeatherExtractor()
        result = extractor.fetch_weather_by_city('InvalidCity', 'XX')
        
        assert result is None
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_timeout(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout()
        
        extractor = WeatherExtractor()
        
        with pytest.raises(requests.exceptions.Timeout):
            extractor.fetch_weather_by_city('London', 'GB')
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_request_exception(self, mock_get):
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        extractor = WeatherExtractor()
        result = extractor.fetch_weather_by_city('London', 'GB')
        
        assert result is None
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_for_cities(self, mock_get, mock_requests_response, sample_cities):
        mock_get.return_value = mock_requests_response
        
        extractor = WeatherExtractor()
        results = extractor.fetch_weather_for_cities(sample_cities)
        
        assert len(results) == len(sample_cities)
        assert mock_get.call_count == len(sample_cities)
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_for_cities_partial_failure(self, mock_get, sample_cities):
        mock_success = Mock()
        mock_success.status_code = 200
        mock_success.json.return_value = {
            'name': 'London',
            'coord': {'lon': 0, 'lat': 0},
            'weather': [{'main': 'Clear', 'description': 'clear', 'icon': '01d'}],
            'main': {'temp': 15, 'feels_like': 14, 'temp_min': 13, 'temp_max': 17, 'pressure': 1013, 'humidity': 65},
            'visibility': 10000,
            'wind': {'speed': 3, 'deg': 180},
            'clouds': {'all': 20},
            'dt': 1609459200,
            'sys': {'country': 'GB'},
            'timezone': 0,
            'cod': 200
        }
        
        mock_fail = Mock()
        mock_fail.status_code = 404
        mock_fail.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_fail)
        
        mock_get.side_effect = [mock_success, mock_fail, mock_success]
        
        extractor = WeatherExtractor()
        results = extractor.fetch_weather_for_cities(sample_cities)
        
        # Should get 2 successful results out of 3
        assert len(results) == 2
    
    @patch('src.extract.requests.get')
    def test_test_api_connection_success(self, mock_get, mock_requests_response):
        mock_get.return_value = mock_requests_response
        
        extractor = WeatherExtractor()
        result = extractor.test_api_connection()
        
        assert result is True
    
    @patch('src.extract.requests.get')
    def test_test_api_connection_failure(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        mock_get.return_value = mock_response
        
        extractor = WeatherExtractor()
        result = extractor.test_api_connection()
        
        assert result is False
    
    @patch('src.extract.requests.get')
    def test_fetch_weather_with_retry_success(self, mock_get, mock_requests_response):
        mock_get.side_effect = [
            requests.exceptions.Timeout(),
            mock_requests_response
        ]
        
        extractor = WeatherExtractor()
        
        # Patch the retry delay to make test faster
        with patch('src.extract.Config.RETRY_DELAY', 0.01):
            result = extractor.fetch_weather_by_city('London', 'GB')
        
        assert result is not None
        assert mock_get.call_count == 2
    
    @patch('src.extract.requests.get')
    def test_api_response_structure(self, mock_get, sample_raw_weather_data):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_raw_weather_data
        mock_get.return_value = mock_response
        
        extractor = WeatherExtractor()
        result = extractor.fetch_weather_by_city('London', 'GB')
        
        # Verify expected fields are present
        assert 'coord' in result
        assert 'weather' in result
        assert 'main' in result
        assert 'wind' in result
        assert 'clouds' in result
        assert 'sys' in result
        assert 'name' in result
        assert 'dt' in result
    
    def test_extractor_timeout_configuration(self):
        extractor = WeatherExtractor()
        assert extractor.timeout == Config.REQUEST_TIMEOUT
    
    @patch('src.extract.requests.get')
    def test_fetch_empty_city_list(self, mock_get):
        extractor = WeatherExtractor()
        results = extractor.fetch_weather_for_cities([])
        
        assert len(results) == 0
        assert mock_get.call_count == 0