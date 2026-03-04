from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from dagster_dbt import DbtCliResource

from nyc_crashes_dagster_dbt import assets
from nyc_crashes_dagster_dbt.resources import PostgresResource
from nyc_crashes_dagster_dbt.assets.dbt_assets import DBT_PROJECT_DIR

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    asset_checks=load_asset_checks_from_modules([assets]),
    resources={
        "postgres": PostgresResource(
            host="localhost",
            port=5432,
            database="nyc_crashes",
        ),
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
