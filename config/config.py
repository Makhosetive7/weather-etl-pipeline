import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    
    API_KEY = os.getenv('OPENWEATHER_API_KEY')
    API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.openweathermap.org/data/2.5/weather')
    
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5433))
    DB_NAME = os.getenv('DB_NAME', 'weather_analytics')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    CITIES = [
        {'name': 'London', 'country': 'GB'},
        {'name': 'New York', 'country': 'US'},
        {'name': 'Tokyo', 'country': 'JP'},
        {'name': 'Paris', 'country': 'FR'},
        {'name': 'Sydney', 'country': 'AU'},
        {'name': 'Mumbai', 'country': 'IN'},
        {'name': 'Dubai', 'country': 'AE'},
        {'name': 'Singapore', 'country': 'SG'},
        {'name': 'Toronto', 'country': 'CA'},
        {'name': 'Berlin', 'country': 'DE'}
    ]
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'logs/weather_etl.log')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    
    @classmethod
    def validate(cls):
        """Validate that required configuration is present"""
        if not cls.API_KEY:
            raise ValueError("OPENWEATHER_API_KEY not set in environment variables")
        
        if not cls.DB_PASSWORD:
            raise ValueError("DB_PASSWORD not set in environment variables")
        
        return True