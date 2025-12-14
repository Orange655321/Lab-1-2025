CREATE TABLE IF NOT EXISTS weather_hourly (
    city               String,
    ts                 DateTime,
    temperature        Float32,
    precipitation      Float32,
    wind_speed         Float32,
    wind_direction     Float32
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (city, ts);
