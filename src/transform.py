import logging
from typing import Dict, List, Optional

from pydantic import ValidationError

from src.models import build_transformed_record, parse_openweather_response

logger = logging.getLogger(__name__)


class WeatherTransformer:

    def __init__(self):
        logger.info("Weather Transformer initialized")

    def transform_single_record(self, raw_data: Dict) -> Optional[Dict]:
        try:
            parsed = parse_openweather_response(raw_data)
            record = build_transformed_record(parsed)
            logger.info(f"Successfully transformed data for {record.city.city_name}")
            return record.to_dict()

        except ValidationError as e:
            logger.error(f"Validation error in raw data: {e}")
            return None

        except ValueError as e:
            logger.error(f"Invalid weather data: {e}")
            return None

        except Exception as e:
            logger.error(f"Error transforming weather data: {e}")
            return None

    def transform_multiple_records(self, raw_data_list: List[Dict]) -> List[Dict]:
        transformed_records = []

        for raw_data in raw_data_list:
            transformed = self.transform_single_record(raw_data)

            if transformed:
                transformed_records.append(transformed)

        logger.info(
            f"Successfully transformed {len(transformed_records)}/{len(raw_data_list)} records"
        )
        return transformed_records

    def validate_transformed_data(self, transformed_data: Dict) -> bool:
        try:
            from src.models import TransformedWeatherRecord

            record = TransformedWeatherRecord.model_validate(transformed_data)
            temp = record.measurement.temperature_celsius
            if temp < -100 or temp > 60:
                logger.warning(f"Temperature out of reasonable range: {temp}°C")
                return False
            return True

        except Exception as e:
            logger.error(f"Error validating transformed data: {e}")
            return False
