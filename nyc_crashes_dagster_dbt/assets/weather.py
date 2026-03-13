import calendar
from datetime import date

from dagster import asset, AssetExecutionContext, MonthlyPartitionsDefinition

from nyc_crashes_dagster_dbt.resources import PostgresResource, OpenMeteoResource, DAILY_VARIABLES

SCHEMA = "raw"
TABLE = "weather_daily_api"

partitions_def = MonthlyPartitionsDefinition(start_date="2012-07-01")

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    date                DATE PRIMARY KEY,
    temperature_2m_max  NUMERIC,
    temperature_2m_min  NUMERIC,
    precipitation_sum   NUMERIC,
    snowfall_sum        NUMERIC,
    snow_depth_max      NUMERIC,
    windspeed_10m_max   NUMERIC,
    windgusts_10m_max   NUMERIC,
    weathercode         INTEGER,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
)
"""


@asset(
    partitions_def=partitions_def,
    group_name="raw",
)
def weather_daily_api(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    open_meteo: OpenMeteoResource,
):
    partition_date = date.fromisoformat(context.partition_key)
    start_date = partition_date.replace(day=1)
    last_day = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date.replace(day=last_day)

    context.log.info(f"Fetching weather {start_date} → {end_date}")
    daily = open_meteo.fetch(start_date.isoformat(), end_date.isoformat())

    rows = list(zip(daily["time"], *[daily[v] for v in DAILY_VARIABLES]))

    with postgres.get_cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(DDL)

    with postgres.get_cursor() as cur:
        cur.execute(
            f"DELETE FROM {SCHEMA}.{TABLE} WHERE date BETWEEN %s AND %s",
            (start_date, end_date),
        )
        cur.executemany(
            f"""
            INSERT INTO {SCHEMA}.{TABLE}
                (date, temperature_2m_max, temperature_2m_min,
                 precipitation_sum, snowfall_sum, snow_depth_max,
                 windspeed_10m_max, windgusts_10m_max, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

    context.log.info(f"Loaded {len(rows)} rows into {SCHEMA}.{TABLE}")
    return len(rows)
