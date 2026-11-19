"""NHL medallion pipeline (Fabric: pl_nhl_etl).

    setup -> land -> bronze.* -> silver.* -> silver.games_teams_star -> gold.*

Tasks and dependencies are generated from ``include/nhl_etl/config.py``:
  * one bronze task per active dataset (runs in parallel once landing is done),
  * one silver task per ``SilverTableConfig``; a silver task waits for its own bronze
    task and for every silver table it has a foreign key to,
  * the star schema runs after all of its silver inputs, then gold.

This file only orchestrates. It imports nothing heavy at module level; all Spark work is
imported inside the task functions so DAG parsing stays fast.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG, TaskGroup, task
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task
    from airflow.utils.task_group import TaskGroup

from include.nhl_etl.config import (
    SILVER_TABLES,
    STAR_INPUTS,
    active_datasets,
    kaggle_datasets,
)

APP = "nhl_etl"


@task
def setup_lakehouse() -> None:
    """Create the folder layout and bronze audit tables (idempotent)."""
    from include.nhl_etl.bootstrap import setup_lakehouse as _setup
    from include.nhl_etl.spark_session import spark_session

    with spark_session(f"{APP}_setup") as spark:
        _setup(spark)


@task
def land(kaggle_dataset: str) -> list[str]:
    """Download one Kaggle dataset and copy the configured files into the landing zone."""
    from include.nhl_etl.landing import land_kaggle_dataset

    return land_kaggle_dataset(kaggle_dataset)


@task
def bronze(dataset_name: str) -> dict:
    """Load one landed CSV into its bronze Delta table (skips unchanged files)."""
    from include.nhl_etl.bronze import run_bronze
    from include.nhl_etl.spark_session import spark_session

    cfg = next(d for d in active_datasets() if d.name == dataset_name)
    with spark_session(f"{APP}_bronze_{dataset_name}") as spark:
        return run_bronze(spark, cfg)


@task
def silver(table_name: str) -> dict:
    """Clean, validate and upsert one bronze table into silver."""
    from include.nhl_etl.silver import get_silver_config, run_silver
    from include.nhl_etl.spark_session import spark_session

    with spark_session(f"{APP}_silver_{table_name}") as spark:
        return run_silver(spark, get_silver_config(table_name))


@task
def games_teams_star() -> dict:
    """Build dim_team and the team/skater/goalie game facts plus champion fact."""
    from include.nhl_etl.spark_session import spark_session
    from include.nhl_etl.star import run_star

    with spark_session(f"{APP}_star") as spark:
        return run_star(spark)


@task
def season_summaries() -> dict:
    """Gold: team, skater and goalie season rollups."""
    from include.nhl_etl.gold import run_season_summaries
    from include.nhl_etl.spark_session import spark_session

    with spark_session(f"{APP}_gold_seasons") as spark:
        return run_season_summaries(spark)


@task
def forecasts() -> dict:
    """Gold: recency-weighted forecasts, backtest and 2020-21 championship odds."""
    from include.nhl_etl.gold import run_forecasts
    from include.nhl_etl.spark_session import spark_session

    with spark_session(f"{APP}_gold_forecasts") as spark:
        return run_forecasts(spark)


with DAG(
    dag_id="nhl_etl",
    description="Kaggle NHL data -> bronze -> silver -> gold (PySpark + Delta)",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # the Kaggle dataset is static; trigger manually (or set a cron here)
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # cap parallel Spark JVMs inside the single Docker container
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["nhl", "pyspark", "delta", "medallion"],
) as dag:
    setup = setup_lakehouse()
    landed = land.expand(kaggle_dataset=kaggle_datasets())

    # ---- bronze: one task per active dataset ----------------------------------------
    bronze_tasks = {}
    with TaskGroup(group_id="bronze"):
        for ds in active_datasets():
            bronze_tasks[ds.name] = bronze.override(task_id=ds.name)(ds.name)
    setup >> landed >> list(bronze_tasks.values())

    # ---- silver: wait for own bronze table + every silver table we reference --------
    silver_tasks = {}
    with TaskGroup(group_id="silver"):
        for cfg in SILVER_TABLES:
            silver_tasks[cfg.name] = silver.override(task_id=cfg.name)(cfg.name)
    for cfg in SILVER_TABLES:
        bronze_tasks[cfg.name] >> silver_tasks[cfg.name]
        for upstream in sorted(cfg.upstream_silver):
            silver_tasks[upstream] >> silver_tasks[cfg.name]

    # ---- star schema (silver) -> gold ------------------------------------------------
    star = games_teams_star()
    [silver_tasks[name] for name in STAR_INPUTS] >> star

    with TaskGroup(group_id="gold"):
        seasons = season_summaries()
        forecast = forecasts()
    star >> seasons >> forecast
