import logging
import time
from functools import wraps
from pathlib import Path
from config.config import Config

def setup_logging():
    log_dir = Path(Config.LOG_FILE).parent
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format=Config.LOG_FORMAT,
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def retry_on_failure(max_retries=3, delay=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                        raise
                    
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s...")
                    time.sleep(delay)
            
        return wrapper
    return decorator

def kelvin_to_celsius(kelvin):
    """Convert Kelvin to Celsius"""
    if kelvin is None:
        return None
    return round(kelvin - 273.15, 2)

def kelvin_to_fahrenheit(kelvin):
    """Convert Kelvin to Fahrenheit"""
    if kelvin is None:
        return None
    return round((kelvin - 273.15) * 9/5 + 32, 2)

def validate_weather_data(data):
    """Validate weather data structure and required fields"""
    required_fields = ['coord', 'main', 'name']
    
    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
    
    temp_celsius = data['main'].get('temp')
    if temp_celsius and (temp_celsius < -100 or temp_celsius > 60):
        raise ValueError(f"Temperature out of reasonable range: {temp_celsius}°C")
    
    return True