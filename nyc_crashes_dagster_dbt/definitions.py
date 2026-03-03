from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

from nyc_crashes_dagster_dbt import assets
from nyc_crashes_dagster_dbt.resources import PostgresResource

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([assets]),
    resources={
        "postgres": PostgresResource(
            host="localhost",
            port=5432,
            database="nyc_crashes",
        ),
    },
)
