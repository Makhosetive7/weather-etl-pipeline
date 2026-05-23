"""FastAPI application exposing weather warehouse analytics."""

from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query

from api import database as db
from api.schemas import (
    CityItem,
    EtlRunItem,
    HealthResponse,
    LatestWeatherItem,
    PaginatedMeasurements,
    TemperatureSummaryItem,
    WeatherExtremes,
)

app = FastAPI(
    title="Weather Analytics API",
    description="Read API for the weather ETL warehouse (star schema).",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.get("/health", response_model=HealthResponse, tags=["health"])
def health_check():
    if db.test_connection():
        return HealthResponse(status="ok", database="connected")
    return HealthResponse(status="degraded", database="disconnected")


@app.get(
    "/api/v1/weather/latest",
    response_model=List[LatestWeatherItem],
    tags=["weather"],
)
def get_latest_weather():
    """Latest measurement per city (mirrors `weather.latest_weather` view)."""
    return db.fetch_latest_weather()


@app.get("/api/v1/cities", response_model=List[CityItem], tags=["cities"])
def list_cities():
    return db.fetch_cities()


@app.get(
    "/api/v1/cities/{city_id}/measurements",
    response_model=PaginatedMeasurements,
    tags=["cities"],
)
def get_city_measurements(
    city_id: int,
    limit: int = Query(100, ge=1, le=500),
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
):
    city = db.fetch_city_by_id(city_id)
    if not city:
        raise HTTPException(status_code=404, detail=f"City {city_id} not found")

    items = db.fetch_measurements(city_id, limit=limit, from_ts=from_ts, to_ts=to_ts)
    return PaginatedMeasurements(
        city_id=city_id,
        city_name=city["city_name"],
        count=len(items),
        items=items,
    )


@app.get(
    "/api/v1/analytics/temperature-summary",
    response_model=List[TemperatureSummaryItem],
    tags=["analytics"],
)
def get_temperature_summary(
    days: int = Query(7, ge=1, le=90, description="Rolling window in days"),
):
    """Average temperature by city (from sql/analytics_examples.sql)."""
    return db.fetch_temperature_summary(days=days)


@app.get(
    "/api/v1/analytics/extremes",
    response_model=WeatherExtremes,
    tags=["analytics"],
)
def get_weather_extremes():
    """Hottest and coldest cities in the latest snapshot."""
    latest = db.fetch_latest_weather()
    if not latest:
        return WeatherExtremes()

    sorted_by_temp = sorted(
        latest,
        key=lambda row: row.get("temperature_celsius") or -999,
        reverse=True,
    )
    return WeatherExtremes(
        hottest=sorted_by_temp[0],
        coldest=sorted_by_temp[-1],
    )


@app.get(
    "/api/v1/analytics/etl-runs",
    response_model=List[EtlRunItem],
    tags=["analytics"],
)
def get_etl_runs(limit: int = Query(10, ge=1, le=50)):
    """Recent ETL pipeline runs."""
    return db.fetch_recent_etl_runs(limit=limit)
