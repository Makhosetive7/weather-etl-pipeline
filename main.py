import sys
import time
import uuid
from datetime import datetime

from config.config import Config
from src.etl_runs import EtlRunTracker
from src.extract import WeatherExtractor
from src.load import WeatherLoader
from src.transform import WeatherTransformer
from src.utils import log_stage, set_log_context, setup_logging

# Exit codes for schedulers / CI
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_PARTIAL = 2


def run_etl_pipeline() -> int:
    run_id = str(uuid.uuid4())
    logger = setup_logging(run_id=run_id)
    set_log_context(run_id=run_id, stage="init")
    run_tracker = EtlRunTracker()

    records_extracted = 0
    records_transformed = 0
    records_loaded = 0
    records_failed = 0
    error_message = None
    loader = None

    log_stage(logger, "init", "Starting Weather ETL Pipeline", run_id=run_id)
    logger.info(f"Run time: {datetime.now()}")

    try:
        Config.validate()
        run_tracker.start_run(run_id)

        loader = WeatherLoader()
        extractor = WeatherExtractor()
        transformer = WeatherTransformer()

        if not loader.test_connection():
            error_message = "Database connection failed"
            logger.error(error_message)
            run_tracker.finish_run(
                run_id,
                "failed",
                error_message=error_message,
            )
            return EXIT_FAILURE

        set_log_context(stage="extract")
        extract_start = time.perf_counter()
        log_stage(
            logger,
            "extract",
            f"Fetching weather data for {len(Config.CITIES)} cities...",
        )
        raw_data_list = extractor.fetch_weather_for_cities(Config.CITIES)
        records_extracted = len(raw_data_list)
        log_stage(
            logger,
            "extract",
            f"Extracted {records_extracted} records",
            duration_ms=round((time.perf_counter() - extract_start) * 1000),
        )

        if not raw_data_list:
            error_message = "No data extracted"
            logger.warning(f"{error_message}. Exiting.")
            run_tracker.finish_run(
                run_id,
                "failed",
                records_extracted=0,
                error_message=error_message,
            )
            return EXIT_FAILURE

        set_log_context(stage="transform")
        transform_start = time.perf_counter()
        log_stage(logger, "transform", "Transforming data...")
        transformed_data_list = transformer.transform_multiple_records(raw_data_list)
        records_transformed = len(transformed_data_list)
        log_stage(
            logger,
            "transform",
            f"Transformed {records_transformed} records",
            duration_ms=round((time.perf_counter() - transform_start) * 1000),
        )

        if not transformed_data_list:
            error_message = "No data transformed"
            logger.warning(f"{error_message}. Exiting.")
            run_tracker.finish_run(
                run_id,
                "failed",
                records_extracted=records_extracted,
                error_message=error_message,
            )
            return EXIT_FAILURE

        set_log_context(stage="load")
        load_start = time.perf_counter()
        log_stage(logger, "load", "Loading data into database...")
        result = loader.load_multiple_records(transformed_data_list)
        records_loaded = result["success"]
        records_failed = result["failure"]
        log_stage(
            logger,
            "load",
            f"Loaded {records_loaded} records, {records_failed} failures",
            duration_ms=round((time.perf_counter() - load_start) * 1000),
        )

        if records_failed > 0 and records_loaded > 0:
            status = "partial"
            exit_code = EXIT_PARTIAL
        elif records_loaded == 0:
            status = "failed"
            exit_code = EXIT_FAILURE
            error_message = "No records loaded"
        else:
            status = "success"
            exit_code = EXIT_SUCCESS

        run_tracker.finish_run(
            run_id,
            status,
            records_extracted=records_extracted,
            records_transformed=records_transformed,
            records_loaded=records_loaded,
            records_failed=records_failed,
            error_message=error_message,
        )

        logger.info("ETL Pipeline Complete")
        logger.info(f"Success: {records_loaded} | Failure: {records_failed}")
        return exit_code

    except Exception as e:
        error_message = str(e)
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        run_tracker.finish_run(
            run_id,
            "failed",
            records_extracted=records_extracted,
            records_transformed=records_transformed,
            records_loaded=records_loaded,
            records_failed=records_failed,
            error_message=error_message,
        )
        return EXIT_FAILURE

    finally:
        if loader:
            loader.close()
        run_tracker.close()


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("WEATHER ETL PIPELINE")
    print("=" * 50 + "\n")

    exit_code = run_etl_pipeline()

    if exit_code == EXIT_SUCCESS:
        print("\n Pipeline completed successfully!")
    elif exit_code == EXIT_PARTIAL:
        print("\n Pipeline completed with partial failures. Check logs.")
    else:
        print("\n Pipeline failed. Check logs for details.")

    sys.exit(exit_code)
