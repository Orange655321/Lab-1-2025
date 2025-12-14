from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from urllib.parse import urlparse
from io import BytesIO

import clickhouse_connect
import pandas as pd
import requests
from dotenv import load_dotenv
from minio import Minio
from prefect import flow, task, get_run_logger

load_dotenv()

OPENMETEO_BASE_URL = os.getenv("OPENMETEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-raw")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CITIES = {
    "Moscow": {"lat": 55.75, "lon": 37.62},
    "Samara": {"lat": 53.20, "lon": 50.15},
}


def get_minio_client() -> Minio:
    parsed = urlparse(MINIO_ENDPOINT)
    endpoint = parsed.netloc or parsed.path
    secure = parsed.scheme == "https"

    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


@task(retries=3, retry_delay_seconds=30)
def fetch_weather(city_name: str, target_day: date) -> dict:
    """Запрос погоды на конкретный день в Open-Meteo."""
    logger = get_run_logger()
    coords = CITIES[city_name]

    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m,precipitation,windspeed_10m,winddirection_10m",
        "timezone": "Europe/Moscow",
        "start_date": target_day.isoformat(),
        "end_date": target_day.isoformat(),
    }

    logger.info(f"Запрашиваем погоду для {city_name} на {target_day}...")
    resp = requests.get(OPENMETEO_BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data


@task
def save_raw_to_minio(city_name: str, target_day: date, data: dict) -> str:
    """Сохраняем сырые JSON-данные в MinIO."""
    logger = get_run_logger()
    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET):
        logger.info(f"Бакет {MINIO_BUCKET} не существует, создаём...")
        client.make_bucket(MINIO_BUCKET)

    object_name = f"{city_name.lower()}-{target_day.isoformat()}.json"
    body = json.dumps(data).encode("utf-8")

    logger.info(f"Сохраняем сырые данные в MinIO как {object_name}...")

    data_stream = BytesIO(body)

    client.put_object(
        MINIO_BUCKET,
        object_name,
        data=data_stream,
        length=len(body),
        content_type="application/json",
    )

    return object_name


@task
def normalize_hourly(city_name: str, target_day: date, data: dict) -> pd.DataFrame:
    """Разворачиваем hourly-данные в табличку для weather_hourly."""
    logger = get_run_logger()

    hourly = data.get("hourly")
    if not hourly:
        logger.warning(f"Нет блока 'hourly' в ответе для {city_name}!")
        return pd.DataFrame()

    df = pd.DataFrame(hourly)
    if df.empty:
        logger.warning(f"Пустой hourly DataFrame для {city_name}")
        return df

    df["ts"] = pd.to_datetime(df["time"])
    df = df[df["ts"].dt.date == target_day]

    if df.empty:
        logger.warning(f"После фильтрации по дате {target_day} данных нет для {city_name}")
        return df

    df["city"] = city_name
    df = df.rename(
        columns={
            "temperature_2m": "temperature",
            "windspeed_10m": "wind_speed",
            "winddirection_10m": "wind_direction",
        }
    )

    df = df[["city", "ts", "temperature", "precipitation", "wind_speed", "wind_direction"]]
    logger.info(f"Нормализовано {len(df)} почасовых записей для {city_name}")
    return df


@task
def aggregate_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Считаем дневную статистику из почасовых значений."""
    logger = get_run_logger()
    if hourly_df.empty:
        logger.warning("Пустой hourly_df, дневная агрегация не будет выполнена")
        return pd.DataFrame()

    df = hourly_df.copy()
    df["date"] = df["ts"].dt.date

    daily = (
        df.groupby(["city", "date"])
        .agg(
            temp_min=("temperature", "min"),
            temp_max=("temperature", "max"),
            temp_avg=("temperature", "mean"),
            precipitation_sum=("precipitation", "sum"),
            max_wind_speed=("wind_speed", "max"),
        )
        .reset_index()
    )

    logger.info(f"Агрегировано дневных записей: {len(daily)}")
    return daily


@task
def load_hourly_to_clickhouse(hourly_df: pd.DataFrame):
    """Загрузка почасовых данных в таблицу weather_hourly."""
    logger = get_run_logger()
    if hourly_df.empty:
        logger.warning("hourly_df пустой, в ClickHouse ничего не пишем")
        return

    client = get_ch_client()
    cols = ["city", "ts", "temperature", "precipitation", "wind_speed", "wind_direction"]
    rows = [list(row) for row in hourly_df[cols].itertuples(index=False)]

    logger.info(f"Вставляем {len(rows)} строк в weather_hourly...")
    client.insert(
        "weather_hourly",
        rows,
        column_names=cols,
    )


@task
def load_daily_to_clickhouse(daily_df: pd.DataFrame):
    """Загрузка дневных агрегатов в таблицу weather_daily."""
    logger = get_run_logger()
    if daily_df.empty:
        logger.warning("daily_df пустой, в ClickHouse ничего не пишем")
        return

    cols = ["city", "date", "temp_min", "temp_max", "temp_avg", "precipitation_sum"]
    daily_for_db = daily_df[cols]

    client = get_ch_client()
    rows = [list(row) for row in daily_for_db.itertuples(index=False)]

    logger.info(f"Вставляем {len(rows)} строк в weather_daily...")
    client.insert(
        "weather_daily",
        rows,
        column_names=cols,
    )


@task
def send_telegram_summary(hourly_all: pd.DataFrame, daily_all: pd.DataFrame, target_day: date):
    """Отправляем краткий прогноз и предупреждения в Telegram (если настроен токен)."""
    logger = get_run_logger()

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or TELEGRAM_BOT_TOKEN.startswith("YOUR_"):
        logger.warning("TELEGRAM_BOT_TOKEN/CHAT_ID не заданы, отправка в Telegram пропущена")
        return

    if daily_all.empty:
        logger.warning("Нет дневных данных для отправки в Telegram")
        return

    messages = []

    for _, row in daily_all.iterrows():
        city = row["city"]
        d = row["date"]
        if hasattr(d, "date"):
            d = d.date()

        mask = (hourly_all["city"] == city) & (hourly_all["ts"].dt.date == d)
        max_wind = hourly_all.loc[mask, "wind_speed"].max()

        text = (
            f"Прогноз на {target_day.isoformat()} для {city}:\n"
            f"  t_min: {row['temp_min']:.1f}°C\n"
            f"  t_max: {row['temp_max']:.1f}°C\n"
            f"  t_avg: {row['temp_avg']:.1f}°C\n"
            f"  Осадки за день: {row['precipitation_sum']:.1f} мм\n"
            f"  Макс. скорость ветра: {max_wind:.1f} м/с"
        )

        warnings = []
        if row["precipitation_sum"] >= 10:
            warnings.append("⚠️ Возможны сильные осадки")
        if max_wind >= 15:
            warnings.append("💨 Сильный ветер")

        if warnings:
            text += "\n" + "\n".join(warnings)

        messages.append(text)

    full_text = "\n\n".join(messages)

    logger.info("Отправляем сообщение в Telegram...")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": full_text})
    if not resp.ok:
        logger.warning(f"Не удалось отправить сообщение в Telegram: {resp.status_code} {resp.text}")



@flow(name="weather_etl")
def weather_etl():
    logger = get_run_logger()
    target_day = date.today() + timedelta(days=1)

    logger.info(f"Старт flow weather_etl, целевой день: {target_day}")

    hourly_dfs = []
    daily_dfs = []

    for city in CITIES.keys():
        logger.info(f"--- Обработка города {city} ---")
        raw = fetch_weather(city, target_day)
        save_raw_to_minio(city, target_day, raw)

        hourly_df = normalize_hourly(city, target_day, raw)
        daily_df = aggregate_daily(hourly_df)

        load_hourly_to_clickhouse(hourly_df)
        load_daily_to_clickhouse(daily_df)

        if not hourly_df.empty:
            hourly_dfs.append(hourly_df)
        if not daily_df.empty:
            daily_dfs.append(daily_df)

    if hourly_dfs and daily_dfs:
        hourly_all = pd.concat(hourly_dfs, ignore_index=True)
        daily_all = pd.concat(daily_dfs, ignore_index=True)
        send_telegram_summary(hourly_all, daily_all, target_day)
    else:
        logger.warning("Нет данных для отправки в Telegram")

    logger.info("Flow weather_etl завершён")


if __name__ == "__main__":
    weather_etl()
