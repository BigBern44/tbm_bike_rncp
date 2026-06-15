"""Définitions Dagster de l'observatoire vélos TBM.

Un asset par étape du pipeline, dont les dépendances reflètent le flux :
raw_* (JSON brut) → parquet_* (Polars typé) → minio_* (upload bronze S3),
puis les assets dbt (silver/gold) déclenchés par sensor à l'arrivée de
nouveaux Parquet sur MinIO.

NB : pas de `from __future__ import annotations` ici — Dagster valide les
annotations de `context` à l'exécution et ne résout pas les annotations
différées (chaînes).
"""

import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Definitions,
    EventLogEntry,
    MaterializeResult,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset,
    asset_sensor,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from ingestion import collect_api, to_parquet
from lake import minio_client

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = REPO_ROOT / "transform" / "dbt_velos"
# dbt est installé dans le même venv que l'interpréteur courant : chemin
# explicite pour ne pas dépendre du PATH du processus Dagster.
DBT_EXECUTABLE = Path(sys.executable).with_name("dbt")

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)
dbt_project.prepare_if_dev()


# --------------------------------------------------------------------------
# Bronze — station_status (toutes les 5 min)
# --------------------------------------------------------------------------

@asset(group_name="bronze_station_status")
def raw_station_status(context: AssetExecutionContext) -> str:
    """Appel GBFS station_status, JSON brut horodaté (bronze landing)."""
    path = collect_api.collect("station_status")
    context.log.info("JSON brut écrit : %s", path)
    return str(path)


@asset(group_name="bronze_station_status")
def minio_station_status(
    context: AssetExecutionContext, raw_station_status: str
) -> MaterializeResult:
    """Conversion Polars typée en mémoire puis upload des Parquet vers MinIO bronze/.

    Aucun Parquet n'est écrit sur le disque local : le JSON brut (landing) reste
    la seule trace locale, le Parquet ne vit que sur l'objet store.
    """
    objects = to_parquet.convert(Path(raw_station_status))
    uris = [minio_client.upload_parquet_bytes(key, data) for key, data in objects]
    context.log.info("Objets téléversés : %s", uris)
    return MaterializeResult(metadata={"uris": uris})


# --------------------------------------------------------------------------
# Bronze — station_information (1×/jour)
# --------------------------------------------------------------------------

@asset(group_name="bronze_station_information")
def raw_station_information(context: AssetExecutionContext) -> str:
    """Appel GBFS station_information, JSON brut horodaté (bronze landing)."""
    path = collect_api.collect("station_information")
    context.log.info("JSON brut écrit : %s", path)
    return str(path)


@asset(group_name="bronze_station_information")
def minio_station_information(
    context: AssetExecutionContext, raw_station_information: str
) -> MaterializeResult:
    """Conversion Polars typée en mémoire puis upload du référentiel vers MinIO bronze/.

    Aucun Parquet local : seul le JSON brut (landing) subsiste sur le disque.
    """
    objects = to_parquet.convert(Path(raw_station_information))
    uris = [minio_client.upload_parquet_bytes(key, data) for key, data in objects]
    context.log.info("Objets téléversés : %s", uris)
    return MaterializeResult(metadata={"uris": uris})


# --------------------------------------------------------------------------
# Silver / Gold — assets dbt (staging puis marts, tests inclus)
# --------------------------------------------------------------------------

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_velos_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Exécute dbt build : modèles stg_/mart_ et leurs tests."""
    yield from dbt.cli(["build"], context=context).stream()


# --------------------------------------------------------------------------
# Jobs, schedules, sensor
# --------------------------------------------------------------------------

job_collecte_status = define_asset_job(
    name="collecte_station_status",
    selection=AssetSelection.assets(raw_station_status, minio_station_status),
)

job_collecte_information = define_asset_job(
    name="collecte_station_information",
    selection=AssetSelection.assets(
        raw_station_information, minio_station_information
    ),
)

job_transformation_dbt = define_asset_job(
    name="transformation_dbt",
    selection=AssetSelection.assets(dbt_velos_assets),
)

schedule_status = ScheduleDefinition(
    job=job_collecte_status,
    cron_schedule="*/60 * * * *",
    name="schedule_station_status_5min",
)

schedule_information = ScheduleDefinition(
    job=job_collecte_information,
    cron_schedule="0 4 * * *",
    name="schedule_station_information_quotidien",
)


@asset_sensor(
    asset_key=AssetKey("minio_station_status"),
    job=job_transformation_dbt,
    name="sensor_nouveau_parquet_bronze",
)
def sensor_nouveau_parquet_bronze(
    context: SensorEvaluationContext, asset_event: EventLogEntry
):
    """Déclenche la transformation dbt à chaque nouveau Parquet sur MinIO."""
    yield RunRequest(run_key=asset_event.run_id)


defs = Definitions(
    assets=[
        raw_station_status,
        minio_station_status,
        raw_station_information,
        minio_station_information,
        dbt_velos_assets,
    ],
    jobs=[job_collecte_status, job_collecte_information, job_transformation_dbt],
    schedules=[schedule_status, schedule_information],
    sensors=[sensor_nouveau_parquet_bronze],
    resources={
        "dbt": DbtCliResource(
            project_dir=dbt_project, dbt_executable=str(DBT_EXECUTABLE)
        )
    },
)
