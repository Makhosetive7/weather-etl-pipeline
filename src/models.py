"""Pydantic models for API responses and transformed warehouse records."""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CoordModel(BaseModel):
    lat: float
    lon: float


class MainWeatherModel(BaseModel):
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int

    @field_validator("temp")
    @classmethod
    def validate_temperature(cls, value: float) -> float:
        if value < -100 or value > 60:
            raise ValueError(f"Temperature out of reasonable range: {value}°C")
        return value


class WeatherItemModel(BaseModel):
    main: str
    description: str
    icon: str


class SysModel(BaseModel):
    country: str


class WindModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    speed: Optional[float] = None
    deg: Optional[int] = None


class CloudsModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    all: Optional[int] = None


class OpenWeatherResponse(BaseModel):
    """Validated shape of OpenWeather current-weather JSON."""

    model_config = ConfigDict(extra="ignore")

    coord: CoordModel
    main: MainWeatherModel
    name: str
    weather: List[WeatherItemModel]
    dt: int
    sys: SysModel
    timezone: int = 0
    visibility: Optional[int] = None
    wind: Optional[WindModel] = None
    clouds: Optional[CloudsModel] = None


class CityRecord(BaseModel):
    city_name: str
    country_code: str
    latitude: float
    longitude: float
    timezone_offset: int = 0


class ConditionRecord(BaseModel):
    main_condition: str
    description: str
    icon_code: str


class MeasurementRecord(BaseModel):
    temperature_celsius: float
    feels_like_celsius: float
    temp_min_celsius: float
    temp_max_celsius: float
    pressure_hpa: int
    humidity_percent: int
    visibility_meters: Optional[int] = None
    wind_speed_mps: Optional[float] = None
    wind_direction_degrees: Optional[int] = None
    cloudiness_percent: Optional[int] = None
    measurement_timestamp: datetime
    api_call_timestamp: datetime = Field(default_factory=datetime.now)

    @field_validator("humidity_percent")
    @classmethod
    def validate_humidity(cls, value: int) -> int:
        if value < 0 or value > 100:
            raise ValueError(f"Humidity out of valid range: {value}%")
        return value


class TransformedWeatherRecord(BaseModel):
    city: CityRecord
    condition: ConditionRecord
    measurement: MeasurementRecord

    def to_dict(self) -> dict[str, Any]:
        """Dict compatible with WeatherLoader (datetimes preserved)."""
        return self.model_dump()


def parse_openweather_response(data: dict) -> OpenWeatherResponse:
    return OpenWeatherResponse.model_validate(data)


def build_transformed_record(raw: OpenWeatherResponse) -> TransformedWeatherRecord:
    weather_info = raw.weather[0]
    wind = raw.wind

    return TransformedWeatherRecord(
        city=CityRecord(
            city_name=raw.name,
            country_code=raw.sys.country,
            latitude=raw.coord.lat,
            longitude=raw.coord.lon,
            timezone_offset=raw.timezone,
        ),
        condition=ConditionRecord(
            main_condition=weather_info.main,
            description=weather_info.description,
            icon_code=weather_info.icon,
        ),
        measurement=MeasurementRecord(
            temperature_celsius=round(raw.main.temp, 2),
            feels_like_celsius=round(raw.main.feels_like, 2),
            temp_min_celsius=round(raw.main.temp_min, 2),
            temp_max_celsius=round(raw.main.temp_max, 2),
            pressure_hpa=raw.main.pressure,
            humidity_percent=raw.main.humidity,
            visibility_meters=raw.visibility,
            wind_speed_mps=wind.speed if wind else None,
            wind_direction_degrees=wind.deg if wind else None,
            cloudiness_percent=raw.clouds.all if raw.clouds else None,
            measurement_timestamp=datetime.fromtimestamp(raw.dt),
        ),
    )
