import json
import logging
import time
from contextvars import ContextVar
from functools import wraps
from pathlib import Path

from config.config import Config

run_id_var: ContextVar[str] = ContextVar("run_id", default="")
stage_var: ContextVar[str] = ContextVar("stage", default="")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": run_id_var.get() or getattr(record, "run_id", ""),
            "stage": stage_var.get() or getattr(record, "stage", ""),
        }
        if hasattr(record, "duration_ms"):
            log_entry["duration_ms"] = record.duration_ms
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def setup_logging(run_id: str = ""):
    log_dir = Path(Config.LOG_FILE).parent
    log_dir.mkdir(exist_ok=True)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, Config.LOG_LEVEL))

    if run_id:
        run_id_var.set(run_id)

    text_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    if Config.STRUCTURED_LOGGING or Config.LOG_FORMAT.lower() == "json":
        formatter = JsonFormatter()
    else:
        fmt = text_format if Config.LOG_FORMAT.lower() == "text" else Config.LOG_FORMAT
        formatter = logging.Formatter(fmt)

    file_handler = logging.FileHandler(Config.LOG_FILE)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    return logging.getLogger(__name__)


def set_log_context(*, run_id: str = "", stage: str = ""):
    if run_id:
        run_id_var.set(run_id)
    if stage:
        stage_var.set(stage)


def log_stage(logger, stage: str, message: str, **extra):
    """Log with stage context; optional duration_ms via extra."""
    set_log_context(stage=stage)
    record_extra = {"stage": stage, **extra}
    if "duration_ms" in record_extra:
        logger.info(message, extra=record_extra)
    else:
        logger.info(message, extra={"stage": stage})


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

                    logger.warning(
                        f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay}s..."
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def kelvin_to_celsius(kelvin):
    """Convert Kelvin to Celsius."""
    if kelvin is None:
        return None
    return round(kelvin - 273.15, 2)


def kelvin_to_fahrenheit(kelvin):
    """Convert Kelvin to Fahrenheit."""
    if kelvin is None:
        return None
    return round((kelvin - 273.15) * 9 / 5 + 32, 2)


def validate_weather_data(data):
    """Validate weather data via Pydantic (raises ValueError on failure)."""
    from src.models import parse_openweather_response

    try:
        parse_openweather_response(data)
        return True
    except Exception as e:
        raise ValueError(str(e)) from e
