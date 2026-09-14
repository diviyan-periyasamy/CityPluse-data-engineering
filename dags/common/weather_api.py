"""Thin wrapper around the Open-Meteo forecast API."""
from datetime import datetime, timezone

import requests

CITIES = {
    "Colombo":  (6.9271, 79.8612),
    "London":   (51.5072, -0.1276),
    "New York": (40.7128, -74.0060),
    "Tokyo":    (35.6895, 139.6917),
    "Sydney":   (-33.8688, 151.2093),
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def get_weather(city_name: str) -> dict:
    """Call Open-Meteo for one city and return a flat dict containing the
    'current' reading plus the city name and a fetch timestamp."""
    if city_name not in CITIES:
        raise ValueError(f"Unknown city: {city_name}")

    lat, lon = CITIES[city_name]
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
        "timezone": "auto",
    }

    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    current = resp.json()["current"]

    return {
        "city_name": city_name,
        "latitude": lat,
        "longitude": lon,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "time": current["time"],
        "temperature_2m": current["temperature_2m"],
        "relative_humidity_2m": current["relative_humidity_2m"],
        "precipitation": current["precipitation"],
        "weather_code": current["weather_code"],
        "wind_speed_10m": current["wind_speed_10m"],
    }


def get_weather_history(city_name: str, days_back: int = 10) -> list[dict]:
    """Fetch real hourly historical weather for the last `days_back` days
    using Open-Meteo's `past_days` parameter. Used by the one-off backfill
    DAG so the pipeline has several days of genuine data to demonstrate
    against without waiting for the hourly DAG to accumulate it live.

    Returns one dict per hour, shaped exactly like get_weather()'s output,
    so it can be published to Kafka through the same code path.
    """
    if city_name not in CITIES:
        raise ValueError(f"Unknown city: {city_name}")

    lat, lon = CITIES[city_name]
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,weather_code,wind_speed_10m",
        "past_days": days_back,
        "forecast_days": 1,  # minimum allowed; we slice it off below
        "timezone": "auto",
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]

    readings = []
    for i, t in enumerate(hourly["time"]):
        readings.append({
            "city_name": city_name,
            "latitude": lat,
            "longitude": lon,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "time": t,
            "temperature_2m": hourly["temperature_2m"][i],
            "relative_humidity_2m": hourly["relative_humidity_2m"][i],
            "precipitation": hourly["precipitation"][i],
            "weather_code": hourly["weather_code"][i],
            "wind_speed_10m": hourly["wind_speed_10m"][i],
        })

    # We asked for forecast_days=1 (the API minimum) on top of past_days,
    # so the response includes one extra day beyond the history we want.
    # Slice it off rather than doing timezone-aware "is this in the future"
    # math per city - simpler and just as correct.
    return readings[: days_back * 24]
