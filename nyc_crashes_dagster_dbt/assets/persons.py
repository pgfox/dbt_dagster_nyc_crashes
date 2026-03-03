import csv
import re
from pathlib import Path
from dagster import asset, AssetExecutionContext
from nyc_crashes_dagster_dbt.resources import PostgresResource

CSV_PATH = Path("data/Motor_Vehicle_Collisions_-_Person.csv")
SCHEMA = "raw"
TABLE = "persons"


def _normalize(col: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', col.strip().lower()).strip('_')


@asset(group_name="raw")
def persons_raw(context: AssetExecutionContext, postgres: PostgresResource):
    with open(CSV_PATH, newline="") as f:
        reader = csv.reader(f)
        raw_headers = next(reader)

    norm_cols = [_normalize(h) for h in raw_headers]
    col_defs = ", ".join(f'"{c}" TEXT' for c in norm_cols)
    col_list = ", ".join(f'"{c}"' for c in norm_cols)

    # Block 1 — DDL (committed immediately)
    with postgres.get_cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} ({col_defs})")
        cur.execute(
            f"ALTER TABLE {SCHEMA}.{TABLE} "
            f"ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ DEFAULT NOW()"
        )

    # Block 2 — Transactional load (rolls back TRUNCATE if COPY fails)
    with postgres.get_cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {SCHEMA}.{TABLE}")

        with open(CSV_PATH, newline="") as f:
            f.readline()
            cur.copy_expert(
                f"COPY {SCHEMA}.{TABLE} ({col_list}) FROM STDIN WITH CSV",
                f,
            )

        cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")
        row_count = cur.fetchone()[0]

    context.log.info(f"Loaded {row_count:,} rows into {SCHEMA}.{TABLE}")
    return row_count
