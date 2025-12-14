from prefect import get_run_logger
from flows.weather_etl import weather_etl


if __name__ == "__main__":
    weather_etl.serve(
        name="weather-etl-daily",
        cron="*/10 * * * *",
        tags=["lab1", "weather"],
        description="Ежедневный ETL: Open-Meteo -> MinIO -> ClickHouse -> Telegram",
    )
