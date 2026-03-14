"""Data quality stage: compute rule flags and log in data_quality_log."""

import sys
import psycopg2

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def log_quality(run_id):
    """Count flagged rows and insert data quality logs."""
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
                rules = [
                    ("temperature_out_of_bounds", "is_outlier_temperature = TRUE"),
                    ("high_precipitation", "is_high_precipitation = TRUE"),
                ]
                logs = []
                for rule_name, condition in rules:
                    q = f"SELECT COUNT(*) FROM fact_sensor_features WHERE run_id = %s AND {condition};"
                    cur.execute(q, (run_id,))
                    flagged = cur.fetchone()[0]
                    logs.append((run_id, "fact_sensor_features", rule_name, flagged))

                # Insert logs
                insert_sql = """
                    INSERT INTO data_quality_log
                    (run_id, table_name, rule_applied, flagged_rows)
                    VALUES (%s, %s, %s, %s);
                """
                for log in logs:
                    cur.execute(insert_sql, log)

                print(f"[data_quality] Logged {len(logs)} rules for run_id={run_id}.")
                return logs
    except psycopg2.Error as exc:
        print(f"[data_quality] DB error: {exc}")
        raise
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    try:
        logs = log_quality(run_id=1)
        for l in logs:
            print(f"[data_quality] {l[2]} flagged {l[3]} rows.")
    except Exception as exc:
        print(f"[data_quality] Failed: {exc}")
        sys.exit(1)
