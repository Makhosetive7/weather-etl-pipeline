import logging
from datetime import datetime
from config.config import Config
from src.utils import setup_logging
from src.extract import WeatherExtractor
from src.transform import WeatherTransformer
from src.load import WeatherLoader

logger = setup_logging()


def run_etl_pipeline():
    logger.info("="*50)
    logger.info("Starting Weather ETL Pipeline")
    logger.info(f"Run time: {datetime.now()}")
    logger.info("="*50)
    
    try:
        Config.validate()
        logger.info("Configuration validated")
        
        extractor = WeatherExtractor()
        transformer = WeatherTransformer()
        loader = WeatherLoader()
        
        if not loader.test_connection():
            logger.error("Database connection failed. Exiting.")
            return False
        
        logger.info("All components initialized")
        
        logger.info(f"Fetching weather data for {len(Config.CITIES)} cities...")
        raw_data_list = extractor.fetch_weather_for_cities(Config.CITIES)
        logger.info(f"Extracted {len(raw_data_list)} records")
        
        if not raw_data_list:
            logger.warning("No data extracted. Exiting.")
            return False
        
        logger.info("Transforming data...")
        transformed_data_list = transformer.transform_multiple_records(raw_data_list)
        logger.info(f"Transformed {len(transformed_data_list)} records")
        
        if not transformed_data_list:
            logger.warning("No data transformed. Exiting.")
            return False
        
        logger.info("Loading data into database...")
        result = loader.load_multiple_records(transformed_data_list)
        
        logger.info("="*50)
        logger.info("ETL Pipeline Complete")
        logger.info(f"Success: {result['success']} records")
        logger.info(f"Failure: {result['failure']} records")
        logger.info(f"Total: {result['total']} records")
        logger.info("="*50)
        
        loader.close()
        
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    print("\n" + "="*50)
    print("WEATHER ETL PIPELINE")
    print("="*50 + "\n")
    
    success = run_etl_pipeline()
    
    if success:
        print("\n Pipeline completed successfully!")
    else:
        print("\n Pipeline failed. Check logs for details.")