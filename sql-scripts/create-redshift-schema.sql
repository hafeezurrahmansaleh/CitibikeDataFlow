-- Dimension Tables
CREATE TABLE stations_dim (
    station_id VARCHAR(255) PRIMARY KEY,
    station_name VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
) DISTSTYLE ALL;

CREATE TABLE users_dim (
    user_id VARCHAR(255) PRIMARY KEY,
    birth_year INT,
    gender VARCHAR(50)
) DISTSTYLE ALL;

CREATE TABLE time_dim (
    time_id TIMESTAMP PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    hour INT,
    day_of_week INT,
    is_weekend BOOLEAN
) DISTSTYLE ALL;

CREATE TABLE bikes_dim (
    bike_id VARCHAR(255) PRIMARY KEY,
    bike_type VARCHAR(50),
    manufacturer VARCHAR(255),
    purchase_date DATE
) DISTSTYLE ALL;

CREATE TABLE weather_dim (
    time_id TIMESTAMP PRIMARY KEY REFERENCES time_dim(time_id),
    temperature_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    weather_condition VARCHAR(255)
) DISTSTYLE ALL;

-- Fact Table (distributed by start_time_id for JOIN efficiency)
CREATE TABLE trips_fact (
    trip_id VARCHAR(255) PRIMARY KEY,
    start_station_id VARCHAR(255) REFERENCES stations_dim(station_id),
    end_station_id VARCHAR(255) REFERENCES stations_dim(station_id),
    start_time_id TIMESTAMP REFERENCES time_dim(time_id),
    end_time_id TIMESTAMP REFERENCES time_dim(time_id),
    user_id VARCHAR(255) REFERENCES users_dim(user_id),
    bike_id VARCHAR(255) REFERENCES bikes_dim(bike_id),
    trip_duration_seconds INT,
    trip_distance_km DOUBLE PRECISION,
    user_type VARCHAR(50)
)
DISTKEY(start_time_id)
SORTKEY(start_time_id);