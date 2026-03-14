"""Transform stage: read raw data, apply cleaning and feature flags, and insert into fact table."""

import sys
import psycopg2
from psycopg2.extras import RealDictCursor

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def transform_run(run_id):
    """Read raw rows for run_id, flag anomalies, and insert into fact_sensor_features."""
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
            with conn.cursor(cursor_factory=RealDictCursor) as cur_read:
                # 1. Read raw rows
                cur_read.execute(
                    "SELECT * FROM raw_sensor_staging WHERE run_id = %s ORDER BY timestamp;",
                    (run_id,),
                )
                rows = cur_read.fetchall()

                if not rows:
                    print(f"[transform] No raw rows found for run_id={run_id}.")
                    return 0, 0

                insert_rows = []
                for r in rows:
                    # Data cleaning: drop rows missing critical timestamp or location
                    ts = r.get("timestamp")
                    location = r.get("location_name")
                    if not ts or not location:
                        continue

                    temp = r.get("temperature_2m")
                    rh = r.get("relative_humidity_2m")
                    precip = r.get("precipitation")

                    # If missing numeric anomalies, keep as null
                    is_outlier_temp = False
                    if temp is not None and (temp < -50 or temp > 60):
                        is_outlier_temp = True

                    is_high_precip = False
                    if precip is not None and precip > 100:
                        is_high_precip = True

                    insert_rows.append((
                        location,
                        ts,
                        temp,
                        rh,
                        precip,
                        is_outlier_temp,
                        is_high_precip,
                        run_id,
                    ))

                if not insert_rows:
                    print("[transform] No cleaned rows to insert.")
                    return 0, 0


            # 2. Insert into fact_sensor_features (all INSERTs in one block)
            with conn.cursor() as cur_write:
                insert_sql = """
                    INSERT INTO fact_sensor_features
                    (location_name, timestamp, temperature_2m, relative_humidity_2m,
                     precipitation, is_outlier_temperature, is_high_precipitation, run_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                for row in insert_rows:
                    cur_write.execute(insert_sql, row)

                inserted = len(insert_rows)


            # 3. Check existing count (separate cursor, no RealDictCursor)
            with conn.cursor() as cur_count:
                cur_count.execute(
                    "SELECT COUNT(*) FROM fact_sensor_features WHERE run_id = %s;",
                    (run_id,),
                )
                row = cur_count.fetchone()
                existing = row[0] if row else 0


            print(f"[transform] Inserted {inserted} fact rows for run_id={run_id}.")
            return inserted, existing

    except psycopg2.Error as exc:
        print(f"[transform] DB error: {exc}")
        raise
    finally:
        if conn:
            conn.close()
