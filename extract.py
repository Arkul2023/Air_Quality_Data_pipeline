"""Extract stage: fetch from Open-Meteo and insert raw rows into raw_sensor_staging."""

import json
import sys
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import execute_values

from config import API_URL, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def get_weather_data(latitude=19.0760, longitude=72.8777):
    """Call Open-Meteo API and return parsed JSON hourly rows."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "relative_humidity_2m", "precipitation"],
        "timezone": "Asia/Kolkata",
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data
    except requests.RequestException as exc:
        print(f"[extract] API request failed: {exc}")
        raise
    except json.JSONDecodeError as exc:
        print(f"[extract] Invalid JSON from API: {exc}")
        raise


def insert_raw_data(run_id, location_name="Mumbai", latitude=19.0760, longitude=72.8777):
    """Insert hourly data into raw_sensor_staging."""
    data = get_weather_data(latitude=latitude, longitude=longitude)
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    rh = hourly.get("relative_humidity_2m", [])
    precip = hourly.get("precipitation", [])

    rows = []
    for i, ts in enumerate(times):
        t = temps[i] if i < len(temps) else None
        h = rh[i] if i < len(rh) else None
        p = precip[i] if i < len(precip) else None
        rows.append((
            location_name,
            latitude,
            longitude,
            datetime.fromisoformat(ts),
            t,
            h,
            p,
            json.dumps(data),
            run_id,
        ))

    if not rows:
        print("[extract] No rows found in API response to insert.")
        return 0

    insert_sql = """
        INSERT INTO raw_sensor_staging
        (location_name, latitude, longitude, timestamp, temperature_2m,
         relative_humidity_2m, precipitation, raw_json, run_id)
        VALUES %s
    """

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
        print(f"[extract] Inserted {len(rows)} raw rows for run_id={run_id}.")
        return len(rows)
    except psycopg2.Error as exc:
        print(f"[extract] DB error: {exc}")
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    try:
        count = insert_raw_data(run_id=1)
        print(f"[extract] Completed with {count} rows inserted.")
    except Exception as exc:
        print(f"[extract] Failed: {exc}")
        sys.exit(1)
