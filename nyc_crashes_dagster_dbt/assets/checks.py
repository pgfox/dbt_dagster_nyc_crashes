from dagster import asset_check, AssetCheckResult, AssetCheckSeverity
from nyc_crashes_dagster_dbt.resources import PostgresResource
from nyc_crashes_dagster_dbt.assets.crashes import crashes_raw
from nyc_crashes_dagster_dbt.assets.persons import persons_raw
from nyc_crashes_dagster_dbt.assets.vehicles import vehicles_raw


@asset_check(asset=crashes_raw, blocking=True)
def crashes_raw_has_rows(postgres: PostgresResource) -> AssetCheckResult:
    with postgres.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.crashes")
        count = cur.fetchone()[0]
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(asset=crashes_raw, blocking=True)
def crashes_raw_no_duplicate_pks(postgres: PostgresResource) -> AssetCheckResult:
    with postgres.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT collision_id FROM raw.crashes"
            "  GROUP BY collision_id HAVING COUNT(*) > 1"
            ") dupes"
        )
        dupe_count = cur.fetchone()[0]
    return AssetCheckResult(
        passed=dupe_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_pk_count": dupe_count},
    )


@asset_check(asset=persons_raw, blocking=True)
def persons_raw_has_rows(postgres: PostgresResource) -> AssetCheckResult:
    with postgres.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.persons")
        count = cur.fetchone()[0]
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(asset=persons_raw, blocking=True)
def persons_raw_no_duplicate_pks(postgres: PostgresResource) -> AssetCheckResult:
    with postgres.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT unique_id FROM raw.persons"
            "  GROUP BY unique_id HAVING COUNT(*) > 1"
            ") dupes"
        )
        dupe_count = cur.fetchone()[0]
    return AssetCheckResult(
        passed=dupe_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_pk_count": dupe_count},
    )


@asset_check(asset=vehicles_raw, blocking=True)
def vehicles_raw_has_rows(postgres: PostgresResource) -> AssetCheckResult:
    with postgres.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.vehicles")
        count = cur.fetchone()[0]
    return AssetCheckResult(
        passed=count > 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(asset=vehicles_raw, blocking=True)
def vehicles_raw_no_duplicate_pks(postgres: PostgresResource) -> AssetCheckResult:
    with postgres.get_cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT unique_id FROM raw.vehicles"
            "  GROUP BY unique_id HAVING COUNT(*) > 1"
            ") dupes"
        )
        dupe_count = cur.fetchone()[0]
    return AssetCheckResult(
        passed=dupe_count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_pk_count": dupe_count},
    )
