from __future__ import annotations

from datetime import datetime, timedelta, date
import requests
from collections import Counter, defaultdict

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException

POSTGRES_CONN_ID = "de_project_connection"

# NYC Open Data 311 dataset endpoint (Socrata)
DATASET_ID = "erm2-nwe9"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
PAGE_SIZE = 5000

# For safety: cap pagination so a single day doesn't run forever
MAX_OFFSET = 500_000


def _completed_day_range() -> tuple[str, str, str]:
    """
    Load the PREVIOUS completed day relative to the DAG run's logical date.
    This avoids partial/incomplete data for "today" and prevents skipped runs at midnight.

    If ds = 2026-02-04, we load 2026-02-03.
    """
    ctx = get_current_context()
    run_day: date = ctx["data_interval_start"].date()
    d: date = run_day - timedelta(days=1)
    next_d = d + timedelta(days=1)

    # Half-open interval: [d 00:00, next_d 00:00)
    start = f"{d.isoformat()}T00:00:00.000"
    end = f"{next_d.isoformat()}T00:00:00.000"
    return d.isoformat(), start, end


def extract_transform(ti):
    load_day, start_ts, end_ts = _completed_day_range()

    offset = 0
    counts = defaultdict(Counter)  # borough -> Counter(complaint_type)

    while True:
        params = {
            "$select": "borough,complaint_type",
            "$where": (
                f"created_date >= '{start_ts}' AND created_date < '{end_ts}' "
                f"AND borough IS NOT NULL AND complaint_type IS NOT NULL"
            ),
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }

        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        rows = r.json()

        if not rows:
            break

        for row in rows:
            b = row.get("borough")
            c = row.get("complaint_type")
            if b and c:
                counts[b][c] += 1

        offset += PAGE_SIZE

        # Safety stop (protects you from runaway pagination)
        if offset > MAX_OFFSET:
            break

    if not counts:
        # For some days the API can be empty / delayed; skip instead of failing the whole DAG
        raise AirflowSkipException(f"No 311 data found for {load_day}")

    # Output ALL counts by borough and complaint type (better for spikes + analytics)
    out = []
    for borough, counter in counts.items():
        for complaint_type, n in counter.items():
            out.append(
                {
                    "load_day": load_day,
                    "borough": borough,
                    "complaint_type": complaint_type,
                    "count": n,
                }
            )

    ti.xcom_push(key="rows", value=out)
    ti.xcom_push(key="load_day", value=load_day)


def load_daily_counts(ti):
    rows = ti.xcom_pull(key="rows", task_ids="extract_transform")
    load_day = ti.xcom_pull(key="load_day", task_ids="extract_transform")
    if not rows or not load_day:
        raise ValueError("Missing transformed rows/day from XCom")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Idempotent: rerun the same day safely
    hook.run(
        "DELETE FROM nyc311_daily_borough_complaint WHERE load_day = %s;",
        parameters=(load_day,),
    )

    hook.insert_rows(
        table="nyc311_daily_borough_complaint",
        rows=[(r["load_day"], r["borough"], r["complaint_type"], r["count"]) for r in rows],
        target_fields=["load_day", "borough", "complaint_type", "count"],
        commit_every=2000,
    )


default_args = {
    "owner": "airflow",
    # You said: start from Jan 2026
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="nyc311_daily_pipeline_with_spikes",
    default_args=default_args,
    schedule="@daily",
    catchup=True,          # ✅ enables backfill from start_date
    max_active_runs=1,     # gentle on the API
    description="NYC 311 Socrata API -> daily counts -> DQ checks -> spikes vs previous day -> Postgres",
)

create_tables = SQLExecuteQueryOperator(
    task_id="create_tables",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    CREATE TABLE IF NOT EXISTS nyc311_daily_borough_complaint (
        load_day DATE NOT NULL,
        borough TEXT NOT NULL,
        complaint_type TEXT NOT NULL,
        count BIGINT NOT NULL,
        PRIMARY KEY (load_day, borough, complaint_type)
    );

    CREATE INDEX IF NOT EXISTS idx_nyc311_daily_day
      ON nyc311_daily_borough_complaint(load_day);

    CREATE INDEX IF NOT EXISTS idx_nyc311_daily_borough
      ON nyc311_daily_borough_complaint(borough);

    CREATE TABLE IF NOT EXISTS nyc311_spikes (
        load_day DATE NOT NULL,
        borough TEXT NOT NULL,
        complaint_type TEXT NOT NULL,
        count_today BIGINT NOT NULL,
        count_yesterday BIGINT NOT NULL,
        abs_change BIGINT NOT NULL,
        pct_change DOUBLE PRECISION,
        PRIMARY KEY (load_day, borough, complaint_type)
    );
    """,
    dag=dag,
)

extract_transform_task = PythonOperator(
    task_id="extract_transform",
    python_callable=extract_transform,
    dag=dag,
)

load_daily_counts_task = PythonOperator(
    task_id="load_daily_counts",
    python_callable=load_daily_counts,
    dag=dag,
)

# Data quality check for the SAME day we loaded: (ds - 1)
data_quality = SQLExecuteQueryOperator(
    task_id="data_quality",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    DO $$
    DECLARE d DATE := '{{ macros.ds_add(ds, -1) }}'::DATE;  -- ✅ loaded day
    DECLARE c BIGINT;
    BEGIN
      SELECT COUNT(*) INTO c
      FROM nyc311_daily_borough_complaint
      WHERE load_day = d;

      IF c = 0 THEN
        RAISE EXCEPTION 'DQ FAIL: no rows loaded for %', d;
      END IF;

      SELECT COUNT(*) INTO c
      FROM nyc311_daily_borough_complaint
      WHERE load_day = d AND (borough IS NULL OR borough = '');

      IF c > 0 THEN
        RAISE EXCEPTION 'DQ FAIL: null/empty borough rows found for %', d;
      END IF;
    END $$;
    """,
    dag=dag,
)

# Compute spikes for the loaded day (ds-1) vs previous day (ds-2)
compute_spikes = SQLExecuteQueryOperator(
    task_id="compute_spikes",
    conn_id=POSTGRES_CONN_ID,
    sql="""
    WITH
    d AS (
      SELECT
        '{{ macros.ds_add(ds, -1) }}'::DATE AS day_today,   -- ✅ loaded day
        '{{ macros.ds_add(ds, -2) }}'::DATE AS day_yday
    ),
    today AS (
      SELECT *
      FROM nyc311_daily_borough_complaint
      WHERE load_day = (SELECT day_today FROM d)
    ),
    yday AS (
      SELECT *
      FROM nyc311_daily_borough_complaint
      WHERE load_day = (SELECT day_yday FROM d)
    ),
    joined AS (
      SELECT
        (SELECT day_today FROM d) AS load_day,
        t.borough,
        t.complaint_type,
        t.count AS count_today,
        COALESCE(y.count, 0) AS count_yesterday,
        (t.count - COALESCE(y.count, 0)) AS abs_change,
        CASE
          WHEN COALESCE(y.count, 0) = 0 THEN NULL
          ELSE (t.count - y.count)::DOUBLE PRECISION / y.count
        END AS pct_change
      FROM today t
      LEFT JOIN yday y
        ON y.borough = t.borough AND y.complaint_type = t.complaint_type
    ),
    deleted AS (
      DELETE FROM nyc311_spikes
      WHERE load_day = (SELECT day_today FROM d)
      RETURNING 1
    )
    INSERT INTO nyc311_spikes
      (load_day, borough, complaint_type, count_today, count_yesterday, abs_change, pct_change)
    SELECT
      load_day, borough, complaint_type, count_today, count_yesterday, abs_change, pct_change
    FROM joined
    WHERE abs_change >= 10       -- tune this (try 5 while testing)
    ORDER BY abs_change DESC
    LIMIT 50;
    """,
    dag=dag,
)

create_tables >> extract_transform_task >> load_daily_counts_task >> data_quality >> compute_spikes