import logging
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config.config import Config

logger = logging.getLogger(__name__)


class WeatherLoader:
    
    def __init__(self):
        try:
            self.engine = create_engine(Config.DATABASE_URL)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def load_city(self, city_data: Dict) -> Optional[int]:
        """Load city data into the database, return city_id"""
        try:
            with self.engine.connect() as conn:
                # Check if city already exists
                select_query = text("""
                    SELECT city_id 
                    FROM weather.cities 
                    WHERE city_name = :city_name 
                    AND country_code = :country_code
                """)
                
                result = conn.execute(
                    select_query,
                    {
                        'city_name': city_data['city_name'],
                        'country_code': city_data['country_code']
                    }
                )
                
                row = result.fetchone()
                
                if row:
                    # City exists, return existing ID
                    return row[0]
                
                # City doesn't exist, insert new city
                insert_query = text("""
                    INSERT INTO weather.cities 
                    (city_name, country_code, latitude, longitude, timezone_offset)
                    VALUES (:city_name, :country_code, :latitude, :longitude, :timezone_offset)
                    RETURNING city_id
                """)
                
                result = conn.execute(
                    insert_query,
                    city_data
                )
                
                conn.commit()
                
                city_id = result.fetchone()[0]
                logger.info(f"Inserted new city: {city_data['city_name']} (ID: {city_id})")
                return city_id
                
        except SQLAlchemyError as e:
            logger.error(f"Database error loading city: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading city: {e}")
            return None
    
    def load_weather_condition(self, condition_data: Dict) -> Optional[int]:
        """Load weather condition data into the database, return condition_id"""
        try:
            with self.engine.connect() as conn:
                # Check if condition already exists
                select_query = text("""
                    SELECT condition_id 
                    FROM weather.weather_conditions 
                    WHERE main_condition = :main_condition 
                    AND description = :description
                """)
                
                result = conn.execute(
                    select_query,
                    {
                        'main_condition': condition_data['main_condition'],
                        'description': condition_data['description']
                    }
                )
                
                row = result.fetchone()
                
                if row:
                    return row[0]
                
                # Condition doesn't exist, insert new condition
                insert_query = text("""
                    INSERT INTO weather.weather_conditions 
                    (main_condition, description, icon_code)
                    VALUES (:main_condition, :description, :icon_code)
                    RETURNING condition_id
                """)
                
                result = conn.execute(
                    insert_query,
                    condition_data
                )
                
                conn.commit()
                
                condition_id = result.fetchone()[0]
                logger.info(f"Inserted new condition: {condition_data['description']} (ID: {condition_id})")
                return condition_id
                
        except SQLAlchemyError as e:
            logger.error(f"Database error loading condition: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading condition: {e}")
            return None
    
    def load_measurement(self, measurement_data: Dict, city_id: int, condition_id: int) -> bool:
        """Load weather measurement data into the database"""
        try:
            with self.engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO weather.weather_measurements 
                    (
                        city_id, 
                        condition_id,
                        temperature_celsius,
                        feels_like_celsius,
                        temp_min_celsius,
                        temp_max_celsius,
                        pressure_hpa,
                        humidity_percent,
                        visibility_meters,
                        wind_speed_mps,
                        wind_direction_degrees,
                        cloudiness_percent,
                        measurement_timestamp
                    )
                    VALUES (
                        :city_id,
                        :condition_id,
                        :temperature_celsius,
                        :feels_like_celsius,
                        :temp_min_celsius,
                        :temp_max_celsius,
                        :pressure_hpa,
                        :humidity_percent,
                        :visibility_meters,
                        :wind_speed_mps,
                        :wind_direction_degrees,
                        :cloudiness_percent,
                        :measurement_timestamp
                    )
                    ON CONFLICT (city_id, measurement_timestamp) DO NOTHING
                """)
                
                # Combine measurement data with foreign keys
                data = {
                    'city_id': city_id,
                    'condition_id': condition_id,
                    **measurement_data
                }
                
                # Remove api_call_timestamp if it exists (not in schema)
                data.pop('api_call_timestamp', None)
                
                conn.execute(insert_query, data)
                conn.commit()
                
                logger.info(f"Inserted measurement for city_id {city_id}")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Database error loading measurement: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading measurement: {e}")
            return False
    
    def load_weather_record(self, transformed_data: Dict) -> bool:
        """Load a complete weather record (city + condition + measurement)"""
        try:
            # Load city
            city_id = self.load_city(transformed_data['city'])
            if not city_id:
                logger.error("Failed to load city")
                return False
            
            # Load weather condition
            condition_id = self.load_weather_condition(transformed_data['condition'])
            if not condition_id:
                logger.error("Failed to load weather condition")
                return False
            
            # Load measurement
            success = self.load_measurement(
                transformed_data['measurement'],
                city_id,
                condition_id
            )
            
            if success:
                logger.info(f"Successfully loaded complete weather record for {transformed_data['city']['city_name']}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error loading weather record: {e}")
            return False
    
    def load_multiple_records(self, transformed_data_list: List[Dict]) -> Dict:
        """Load multiple weather records and return success/failure counts"""
        success_count = 0
        failure_count = 0
        
        for data in transformed_data_list:
            if self.load_weather_record(data):
                success_count += 1
            else:
                failure_count += 1
        
        logger.info(f"Loaded {success_count} records, {failure_count} failures")
        
        return {
            'success': success_count,
            'failure': failure_count,
            'total': len(transformed_data_list)
        }
    
    def test_connection(self) -> bool:
        """Test the database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info(" Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


# Test code
if __name__ == "__main__":
    from src.utils import setup_logging
    from src.extract import WeatherExtractor
    from src.transform import WeatherTransformer
    
    # Setup logging
    setup_logging()
    
    # Create instances
    extractor = WeatherExtractor()
    transformer = WeatherTransformer()
    loader = WeatherLoader()
    
    # Test database connection
    if not loader.test_connection():
        print(" Database connection failed")
        exit(1)
    
    print(" Database connected")
    
    # Extract data
    raw_data = extractor.fetch_weather_by_city("Paris", "FR")
    
    if raw_data:
        # Transform data
        transformed = transformer.transform_single_record(raw_data)
        
        if transformed:
            # Load into database
            success = loader.load_weather_record(transformed)
            
            if success:
                print("\n Successfully loaded weather data into database!")
                print(f"City: {transformed['city']['city_name']}")
                print(f"Temperature: {transformed['measurement']['temperature_celsius']}°C")
            else:
                print("\n Failed to load data into database")
        else:
            print("\n Transformation failed")
    else:
        print("\n Failed to fetch data")
    
    loader.close()