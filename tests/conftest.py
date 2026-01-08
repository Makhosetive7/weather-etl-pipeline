import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock


@pytest.fixture
def sample_raw_weather_data():
    
    return {
        'coord': {'lon': -0.1257, 'lat': 51.5085},
        'weather': [
            {
                'id': 800,
                'main': 'Clear',
                'description': 'clear sky',
                'icon': '01d'
            }
        ],
        'base': 'stations',
        'main': {
            'temp': 15.5,
            'feels_like': 14.2,
            'temp_min': 13.0,
            'temp_max': 17.0,
            'pressure': 1013,
            'humidity': 65
        },
        'visibility': 10000,
        'wind': {
            'speed': 3.5,
            'deg': 180
        },
        'clouds': {
            'all': 20
        },
        'dt': 1609459200,  # 2021-01-01 00:00:00 UTC
        'sys': {
            'type': 2,
            'id': 2019646,
            'country': 'GB',
            'sunrise': 1609485600,
            'sunset': 1609519200
        },
        'timezone': 0,
        'id': 2643743,
        'name': 'London',
        'cod': 200
    }


@pytest.fixture
def sample_transformed_data():
    return {
        'city': {
            'city_name': 'London',
            'country_code': 'GB',
            'latitude': 51.5085,
            'longitude': -0.1257,
            'timezone_offset': 0
        },
        'condition': {
            'main_condition': 'Clear',
            'description': 'clear sky',
            'icon_code': '01d'
        },
        'measurement': {
            'temperature_celsius': 15.5,
            'feels_like_celsius': 14.2,
            'temp_min_celsius': 13.0,
            'temp_max_celsius': 17.0,
            'pressure_hpa': 1013,
            'humidity_percent': 65,
            'visibility_meters': 10000,
            'wind_speed_mps': 3.5,
            'wind_direction_degrees': 180,
            'cloudiness_percent': 20,
            'measurement_timestamp': datetime.fromtimestamp(1609459200),
            'api_call_timestamp': datetime.now()
        }
    }


@pytest.fixture
def sample_cities():
    return [
        {'name': 'London', 'country': 'GB'},
        {'name': 'Paris', 'country': 'FR'},
        {'name': 'Tokyo', 'country': 'JP'}
    ]


@pytest.fixture
def mock_requests_response():
    """Mock successful requests response"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
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
        'visibility': 10000,
        'wind': {'speed': 3.5, 'deg': 180},
        'clouds': {'all': 20},
        'dt': 1609459200,
        'sys': {'country': 'GB'},
        'timezone': 0,
        'name': 'London',
        'cod': 200
    }
    return mock_response


@pytest.fixture
def mock_db_connection():
    
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = [1]
    mock_conn.execute.return_value = mock_result
    return mock_conn


@pytest.fixture
def mock_engine():

    mock_eng = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = [1]
    mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=False)
    mock_eng.connect.return_value = mock_conn
    return mock_eng


@pytest.fixture
def invalid_weather_data():

    return {
        'name': 'London',
        # Missing required 'coord' field
        'main': {
            'temp': 15.5
        }
    }


@pytest.fixture
def extreme_temperature_data():

    return {
        'coord': {'lon': -0.1257, 'lat': 51.5085},
        'weather': [{'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}],
        'main': {
            'temp': 150.0,  # Extreme temperature
            'feels_like': 150.0,
            'temp_min': 150.0,
            'temp_max': 150.0,
            'pressure': 1013,
            'humidity': 65
        },
        'visibility': 10000,
        'wind': {'speed': 3.5, 'deg': 180},
        'clouds': {'all': 20},
        'dt': 1609459200,
        'sys': {'country': 'GB'},
        'timezone': 0,
        'name': 'TestCity',
        'cod': 200
    }