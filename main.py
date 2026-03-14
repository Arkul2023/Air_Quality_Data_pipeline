"""Run the full pipeline: extract -> transform -> data_quality."""

import sys
import traceback

from extract import insert_raw_data
from transform import transform_run
from data_quality import log_quality
from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def create_tables_if_needed():
    """Create tables by running SQL from create_tables.sql if they do not exist."""
    import psycopg2
    from pathlib import Path

    sql_file = Path(__file__).resolve().parent / "sql" / "create_tables.sql"
    with open(sql_file, "r", encoding="utf-8") as f:
        ddl = f.read()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        print("[main] Ensured tables exist.")
    finally:
        conn.close()


def run_pipeline(run_id=1):
    print(f"[main] Starting run_id={run_id}...")
    inserted_raw = insert_raw_data(run_id=run_id)
    inserted_fact, _ = transform_run(run_id=run_id)
    dq_logs = log_quality(run_id=run_id)

    print(f"Run {run_id} completed: {inserted_raw} rows inserted into raw_sensor_staging.")
    print(f"Run {run_id} completed: {inserted_fact} rows inserted into fact_sensor_features.")
    for rule_name, flagged in [(log[2], log[3]) for log in dq_logs]:
        print(f"Run {run_id} completed: {flagged} rows flagged as {rule_name}.")


if __name__ == "__main__":
    run_id = 1
    if len(sys.argv) > 1:
        try:
            run_id = int(sys.argv[1])
        except ValueError:
            print("Usage: python main.py [run_id]")
            sys.exit(1)

    try:
        create_tables_if_needed()
        run_pipeline(run_id=run_id)
    except Exception as exc:
        print(f"[main] Pipeline failed with exception type: {type(exc)}")
        print(f"[main] Exception message: {exc}")
        print("[main] Full traceback:")
        traceback.print_exc()
        sys.exit(1)
