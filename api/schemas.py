"""API response schemas."""

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    database: str


class LatestWeatherItem(BaseModel):
    city_id: int
    city_name: str
    country_code: Optional[str] = None
    temperature_celsius: Optional[float] = None
    feels_like_celsius: Optional[float] = None
    humidity_percent: Optional[int] = None
    wind_speed_mps: Optional[float] = None
    main_condition: Optional[str] = None
    description: Optional[str] = None
    measurement_timestamp: datetime


class CityItem(BaseModel):
    city_id: int
    city_name: str
    country_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class MeasurementItem(BaseModel):
    measurement_id: int
    city_id: int
    temperature_celsius: Optional[float] = None
    feels_like_celsius: Optional[float] = None
    humidity_percent: Optional[int] = None
    wind_speed_mps: Optional[float] = None
    main_condition: Optional[str] = None
    description: Optional[str] = None
    measurement_timestamp: datetime


class TemperatureSummaryItem(BaseModel):
    city_name: str
    country_code: Optional[str] = None
    avg_temp_c: float
    measurement_count: int


class WeatherExtremes(BaseModel):
    hottest: Optional[LatestWeatherItem] = None
    coldest: Optional[LatestWeatherItem] = None


class EtlRunItem(BaseModel):
    run_id: UUID
    status: str
    records_extracted: int
    records_transformed: int
    records_loaded: int
    records_failed: int
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None


class PaginatedMeasurements(BaseModel):
    city_id: int
    city_name: str
    count: int
    items: List[MeasurementItem]
