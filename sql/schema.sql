-- Create schema
CREATE SCHEMA IF NOT EXISTS weather;

-- City Dimension Table
-- Stores information about cities we're tracking
CREATE TABLE IF NOT EXISTS weather.cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country_code VARCHAR(10),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    timezone_offset INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_name, country_code)
);

-- Weather Condition Dimension Table
-- Stores weather condition types (sunny, cloudy, rainy, etc.)
CREATE TABLE IF NOT EXISTS weather.weather_conditions (
    condition_id SERIAL PRIMARY KEY,
    main_condition VARCHAR(50),
    description VARCHAR(200),
    icon_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(main_condition, description)
);

-- Weather Measurements Fact Table
-- Stores the actual weather measurements
CREATE TABLE IF NOT EXISTS weather.weather_measurements (
    measurement_id SERIAL PRIMARY KEY,
    city_id INT REFERENCES weather.cities(city_id),
    condition_id INT REFERENCES weather.weather_conditions(condition_id),
    
    -- Temperature metrics (Celsius)
    temperature_celsius DECIMAL(5,2),
    feels_like_celsius DECIMAL(5,2),
    temp_min_celsius DECIMAL(5,2),
    temp_max_celsius DECIMAL(5,2),
    
    -- Atmospheric conditions
    pressure_hpa INT,
    humidity_percent INT,
    visibility_meters INT,
    
    -- Wind conditions
    wind_speed_mps DECIMAL(5,2),
    wind_direction_degrees INT,
    
    -- Cloud coverage
    cloudiness_percent INT,
    
    -- Timestamps
    measurement_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Prevent duplicate measurements for same city and time
    UNIQUE(city_id, measurement_timestamp)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_measurements_city 
    ON weather.weather_measurements(city_id);

CREATE INDEX IF NOT EXISTS idx_measurements_timestamp 
    ON weather.weather_measurements(measurement_timestamp DESC);

-- Create a view for latest weather per city
CREATE OR REPLACE VIEW weather.latest_weather AS
SELECT 
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
);

-- Success message
SELECT 'Weather Analytics Schema Created Successfully!' AS status;