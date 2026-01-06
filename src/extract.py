import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime
from config.config import Config
from src.utils import retry_on_failure

logger = logging.getLogger(__name__)


class WeatherExtractor:
    def __init__(self):
        self.api_key = Config.API_KEY
        self.base_url = Config.API_BASE_URL
        self.timeout = Config.REQUEST_TIMEOUT

        if not self.api_key:
            raise ValueError("API key not configured properly. Please check OPENWEATHER_API_KEY in the .env file")
        
        logger.info("Weather Extractor initialized")

    @retry_on_failure(max_retries=Config.MAX_RETRIES, delay=Config.RETRY_DELAY)
    def fetch_weather_by_city(self, city_name: str, country_code: str = None) -> Optional[Dict]:
        
        try:
            query = f"{city_name},{country_code}" if country_code else city_name

            params = {
                "q": query,
                "appid": self.api_key,
                "units": "metric"
            }

            logger.info(f"Fetching weather data for: {query}")

            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )

            response.raise_for_status()

            data = response.json()

            logger.info(f"Successfully fetched weather data for: {query}")
            return data
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Invalid API KEY. Please check your Weather API key in the .env file")
            elif e.response.status_code == 404:
                logger.error(f"City not found: {query}")
            else:
                logger.error(f"HTTP error occurred: {e}")
            return None
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {query}")
            raise
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data for {query}: {e}")
            return None

    def fetch_weather_for_cities(self, cities: List[Dict]) -> List[Dict]:
        
        weather_data = []
        
        for city in cities:
            try:
                city_name = city.get("name")
                country_code = city.get("country")
                
                data = self.fetch_weather_by_city(city_name, country_code)
                
                if data:
                    weather_data.append(data)
                    logger.info(f"Fetched data for {city_name}")
                    
            except Exception as e:
                logger.warning(f"Failed to fetch data for {city_name}: {str(e)}")
                continue
            
        logger.info(f"Successfully fetched weather data for {len(weather_data)}/{len(cities)} cities")
        return weather_data

    def test_api_connection(self) -> bool:
        
        try:
            logger.info("Testing API Connection...")
            data = self.fetch_weather_by_city("London", "GB")
            
            if data:
                logger.info("API connection successful")
                return True
            else:
                logger.error("API connection test failed")
                return False
            
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False


if __name__ == "__main__":
    from src.utils import setup_logging
    
    setup_logging()
    
    extractor = WeatherExtractor()
    
    if extractor.test_api_connection():
        print("\n API is working!\n")
        
        weather = extractor.fetch_weather_by_city("New York", "US")
        
        if weather:
            print(f"City: {weather['name']}")
            print(f"Temperature: {weather['main']['temp']}°C")
            print(f"Feels Like: {weather['main']['feels_like']}°C")
            print(f"Humidity: {weather['main']['humidity']}%")
            print(f"Weather: {weather['weather'][0]['description']}")
    else:
        print("\n API connection failed. Please check your API key.")