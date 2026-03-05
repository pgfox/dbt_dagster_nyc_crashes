from pathlib import Path
from dagster_dbt import DbtProject, dbt_assets, DbtCliResource, DagsterDbtTranslator

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

nyc_crashes_dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)
nyc_crashes_dbt_project.prepare_if_dev()


# class _StagingTranslator(DagsterDbtTranslator):
#     def get_group_name(self, dbt_resource_props: dict) -> str:
#         return "staging"


@dbt_assets(
    manifest=nyc_crashes_dbt_project.manifest_path,
    # dagster_dbt_translator=_StagingTranslator(),
)
def nyc_crashes_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
