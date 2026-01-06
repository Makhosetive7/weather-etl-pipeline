import logging
from typing import Dict, List, Optional
from datetime import datetime
from src.utils import kelvin_to_celsius, validate_weather_data

logger = logging.getLogger(__name__)


class weather_transformer:
    
    def __init__(self):
        logger.info("Weather Transformer initialized")
    
    def transform_single_record(self, raw_data: Dict) -> Optional[Dict]:
        
        try:

            validate_weather_data(raw_data)
            
            city_data = {
                'city_name': raw_data['name'],
                'country_code': raw_data['sys']['country'],
                'latitude': raw_data['coord']['lat'],
                'longitude': raw_data['coord']['lon'],
                'timezone_offset': raw_data.get('timezone', 0)
            }
            
            weather_info = raw_data['weather'][0]
            condition_data = {
                'main_condition': weather_info['main'],
                'description': weather_info['description'],
                'icon_code': weather_info['icon']
            }
            
            main_data = raw_data['main']
            wind_data = raw_data.get('wind', {})
            
            measurement_data = {
                'temperature_celsius': round(main_data['temp'], 2),
                'feels_like_celsius': round(main_data['feels_like'], 2),
                'temp_min_celsius': round(main_data['temp_min'], 2),
                'temp_max_celsius': round(main_data['temp_max'], 2),
                
                'pressure_hpa': main_data['pressure'],
                'humidity_percent': main_data['humidity'],
                'visibility_meters': raw_data.get('visibility'),
                
                'wind_speed_mps': wind_data.get('speed'),
                'wind_direction_degrees': wind_data.get('deg'),
                
                'cloudiness_percent': raw_data.get('clouds', {}).get('all'),
                
                'measurement_timestamp': datetime.fromtimestamp(raw_data['dt']),
                'api_call_timestamp': datetime.now()
            }
            
            transformed_data = {
                'city': city_data,
                'condition': condition_data,
                'measurement': measurement_data
            }
            
            logger.info(f"Successfully transformed data for {city_data['city_name']}")
            return transformed_data
            
        except KeyError as e:
            logger.error(f"Missing required field in raw data: {e}")
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
        
        logger.info(f"Successfully transformed {len(transformed_records)}/{len(raw_data_list)} records")
        return transformed_records
    
    def validate_transformed_data(self, transformed_data: Dict) -> bool:
        
        try:
            required_sections = ['city', 'condition', 'measurement']
            for section in required_sections:
                if section not in transformed_data:
                    logger.error(f"Missing required section: {section}")
                    return False
            
            temp = transformed_data['measurement']['temperature_celsius']
            if temp < -100 or temp > 60:
                logger.warning(f"Temperature out of reasonable range: {temp}°C")
                return False
            
            humidity = transformed_data['measurement']['humidity_percent']
            if humidity < 0 or humidity > 100:
                logger.warning(f"Humidity out of valid range: {humidity}%")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating transformed data: {e}")
            return False


if __name__ == "__main__":
    from src.utils import setup_logging
    from src.extract import weather_extractor
    
    setup_logging()
    
    extractor = weather_extractor()
    transformer = weather_transformer()
    
    raw_data = extractor.fetch_weather_by_city("Tokyo", "JP")
    
    if raw_data:
        transformed = transformer.transform_single_record(raw_data)
        
        if transformed and transformer.validate_transformed_data(transformed):
            print("\n✓ Transformation successful!\n")
            print(f"City: {transformed['city']['city_name']}, {transformed['city']['country_code']}")
            print(f"Temperature: {transformed['measurement']['temperature_celsius']}°C")
            print(f"Condition: {transformed['condition']['description']}")
        else:
            print("\n✗ Transformation failed or validation error")
    else:
        print("\n✗ Failed to fetch data")