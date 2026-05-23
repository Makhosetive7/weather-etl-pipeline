"""Database access for the read API."""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.config import Config

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(Config.DATABASE_URL)
    return _engine


def test_connection() -> bool:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def fetch_latest_weather() -> List[dict]:
    query = text(
        """
        SELECT
            c.city_id,
            c.city_name,
            c.country_code,
            wm.temperature_celsius,
            wm.feels_like_celsius,
            wm.humidity_percent,
            wm.wind_speed_mps,
            wc.main_condition,
            wc.description,
            wm.measurement_timestamp
        FROM weather.weather_measurements wm
        JOIN weather.cities c ON wm.city_id = c.city_id
        JOIN weather.weather_conditions wc ON wm.condition_id = wc.condition_id
        WHERE wm.measurement_timestamp = (
            SELECT MAX(measurement_timestamp)
            FROM weather.weather_measurements wm2
            WHERE wm2.city_id = wm.city_id
        )
        ORDER BY c.city_name
        """
    )
    with get_engine().connect() as conn:
        result = conn.execute(query)
        return [dict(row._mapping) for row in result]


def fetch_cities() -> List[dict]:
    query = text(
        """
        SELECT city_id, city_name, country_code, latitude, longitude
        FROM weather.cities
        ORDER BY city_name
        """
    )
    with get_engine().connect() as conn:
        result = conn.execute(query)
        return [dict(row._mapping) for row in result]


def fetch_city_by_id(city_id: int) -> Optional[dict]:
    query = text(
        """
        SELECT city_id, city_name, country_code, latitude, longitude
        FROM weather.cities
        WHERE city_id = :city_id
        """
    )
    with get_engine().connect() as conn:
        row = conn.execute(query, {"city_id": city_id}).fetchone()
        return dict(row._mapping) if row else None


def fetch_measurements(
    city_id: int,
    *,
    limit: int = 100,
    from_ts: Optional[datetime] = None,
    to_ts: Optional[datetime] = None,
) -> List[dict]:
    conditions = ["wm.city_id = :city_id"]
    params = {"city_id": city_id, "limit": limit}

    if from_ts:
        conditions.append("wm.measurement_timestamp >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts:
        conditions.append("wm.measurement_timestamp <= :to_ts")
        params["to_ts"] = to_ts

    where_clause = " AND ".join(conditions)
    query = text(
        f"""
        SELECT
            wm.measurement_id,
            wm.city_id,
            wm.temperature_celsius,
            wm.feels_like_celsius,
            wm.humidity_percent,
            wm.wind_speed_mps,
            wc.main_condition,
            wc.description,
            wm.measurement_timestamp
        FROM weather.weather_measurements wm
        JOIN weather.weather_conditions wc ON wm.condition_id = wc.condition_id
        WHERE {where_clause}
        ORDER BY wm.measurement_timestamp DESC
        LIMIT :limit
        """
    )
    with get_engine().connect() as conn:
        result = conn.execute(query, params)
        return [dict(row._mapping) for row in result]


def fetch_temperature_summary(days: int = 7) -> List[dict]:
    query = text(
        """
        SELECT
            c.city_name,
            c.country_code,
            ROUND(AVG(wm.temperature_celsius)::numeric, 2) AS avg_temp_c,
            COUNT(*)::int AS measurement_count
        FROM weather.weather_measurements wm
        JOIN weather.cities c ON wm.city_id = c.city_id
        WHERE wm.measurement_timestamp >= NOW() - MAKE_INTERVAL(days => :days)
        GROUP BY c.city_name, c.country_code
        ORDER BY avg_temp_c DESC
        """
    )
    with get_engine().connect() as conn:
        result = conn.execute(query, {"days": days})
        return [dict(row._mapping) for row in result]


def fetch_recent_etl_runs(limit: int = 10) -> List[dict]:
    query = text(
        """
        SELECT
            run_id,
            status,
            records_extracted,
            records_transformed,
            records_loaded,
            records_failed,
            started_at,
            finished_at,
            EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds
        FROM weather.etl_runs
        ORDER BY started_at DESC
        LIMIT :limit
        """
    )
    with get_engine().connect() as conn:
        result = conn.execute(query, {"limit": limit})
        return [dict(row._mapping) for row in result]
