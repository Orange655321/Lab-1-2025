CREATE TABLE IF NOT EXISTS weather_daily (
    city               String,
    date               Date,
    temp_min           Float32,
    temp_max           Float32,
    temp_avg           Float32,
    precipitation_sum  Float32
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (city, date);
