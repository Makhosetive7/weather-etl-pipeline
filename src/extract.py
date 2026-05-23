import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests

from config.config import Config
from src.utils import retry_on_failure

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe minimum delay between API calls."""

    def __init__(self, delay_seconds: float):
        self.delay = max(0.0, delay_seconds)
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self):
        if self.delay <= 0:
            return
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self._last_call = time.time()


class WeatherExtractor:
    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.api_key = Config.API_KEY
        self.base_url = Config.API_BASE_URL
        self.timeout = Config.REQUEST_TIMEOUT
        self.rate_limiter = rate_limiter or RateLimiter(Config.API_RATE_LIMIT_DELAY)

        if not self.api_key:
            raise ValueError(
                "API key not configured properly. Please check OPENWEATHER_API_KEY in the .env file"
            )

        logger.info("Weather Extractor initialized")

    @retry_on_failure(max_retries=Config.MAX_RETRIES, delay=Config.RETRY_DELAY)
    def fetch_weather_by_city(self, city_name: str, country_code: str = None) -> Optional[Dict]:
        self.rate_limiter.wait()

        try:
            query = f"{city_name},{country_code}" if country_code else city_name

            params = {"q": query, "appid": self.api_key, "units": "metric"}

            logger.info(f"Fetching weather data for: {query}")

            response = requests.get(self.base_url, params=params, timeout=self.timeout)

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

    def _fetch_city_safe(self, city: Dict) -> Optional[Dict]:
        city_name = city.get("name")
        country_code = city.get("country")
        try:
            data = self.fetch_weather_by_city(city_name, country_code)
            if data:
                logger.info(f"Fetched data for {city_name}")
            return data
        except Exception as e:
            logger.warning(f"Failed to fetch data for {city_name}: {str(e)}")
            return None

    def fetch_weather_for_cities(
        self,
        cities: List[Dict],
        max_workers: Optional[int] = None,
    ) -> List[Dict]:
        workers = max_workers or Config.EXTRACT_WORKERS
        weather_data = []

        if workers <= 1 or len(cities) <= 1:
            for city in cities:
                data = self._fetch_city_safe(city)
                if data:
                    weather_data.append(data)
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(self._fetch_city_safe, city): city for city in cities}
                for future in as_completed(futures):
                    data = future.result()
                    if data:
                        weather_data.append(data)

        logger.info(
            f"Successfully fetched weather data for {len(weather_data)}/{len(cities)} cities"
        )
        return weather_data

    def test_api_connection(self) -> bool:
        try:
            logger.info("Testing API Connection...")
            data = self.fetch_weather_by_city("London", "GB")

            if data:
                logger.info("API connection successful")
                return True
            logger.error("API connection test failed")
            return False

        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False
