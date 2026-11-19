"""Bronze -> silver (Fabric: nb_03_1_* / nb_03_2_* / nb_03_3_* notebooks).

One generic runner applies the same steps to every table, driven by
``config.SilverTableConfig``:

  read new bronze rows -> keep latest per key -> fill nulls -> cast dates/timestamps
  -> drop foreign-key orphans -> quality checks (stop the load on failure)
  -> upsert (SCD Type 1)
"""
from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from include.nhl_etl.config import SILVER_TABLES, SilverTableConfig
from include.nhl_etl.dbutils import (
    drop_foreign_key_violations,
    keep_latest_per_key,
    load_table,
    read_new_bronze_rows,
    validate_column_values,
    validate_no_nulls,
    validate_primary_keys,
    write_to_silver,
)

logger = logging.getLogger(__name__)


def validate_game_date_to_season(df: DataFrame) -> DataFrame:
    """Game timestamps must be present and fall inside their season's date range.

    A season is assumed to run from Sept 15 of the start year to Sept 30 of the end year
    (``season`` looks like ``20102011``).
    """
    if (null_count := df.filter(F.col("date_time_GMT").isNull()).count()) > 0:
        raise RuntimeError(
            f"Found {null_count} row(s) with a null or unparseable date_time_GMT value."
        )

    season_start = F.substring("season", 1, 4).cast("int")
    season_end = F.substring("season", 5, 4).cast("int")
    start_date = F.to_timestamp(F.concat(season_start, F.lit("-09-15 00:00:00")))
    end_date = F.to_timestamp(F.concat(season_end, F.lit("-09-30 23:59:59")))

    invalid_count = df.filter(
        (F.col("date_time_GMT") < start_date) | (F.col("date_time_GMT") > end_date)
    ).count()
    if invalid_count > 0:
        raise RuntimeError(
            f"Found {invalid_count} row(s) with date_time_GMT outside the corresponding season range."
        )
    return df


# Table-specific checks referenced by name from SilverTableConfig.validators
VALIDATORS: dict[str, Callable[[DataFrame], DataFrame]] = {
    "game_date_to_season": validate_game_date_to_season,
}


def run_silver(spark: SparkSession, cfg: SilverTableConfig) -> dict:
    """Clean, validate and upsert one bronze table into silver."""
    pk = list(cfg.primary_keys)

    # only new bronze rows since the last silver load
    df = read_new_bronze_rows(spark, cfg.bronze_table, cfg.silver_table, columns=list(cfg.required_cols))
    if df.isEmpty():
        logger.info("No new data for %s", cfg.silver_table)
        return {"silver_table": cfg.silver_table, "status": "no_new_data"}

    # one row per business key, newest wins
    df = keep_latest_per_key(df, pk)

    # cleaning / transforms
    if cfg.fill_null:
        df = df.fillna(cfg.fill_null)
    for col in cfg.date_cols:
        df = df.withColumn(col, F.to_date(F.col(col)))
    for col in cfg.timestamp_cols:
        df = df.withColumn(col, F.to_timestamp(F.col(col)))

    # referential integrity against already-cleaned silver tables (orphans are dropped)
    for fk in cfg.foreign_keys:
        ref = load_table(spark, f"silver.{fk.ref_table}", [fk.ref_column])
        df = drop_foreign_key_violations(df, ftable=ref, keys=list(fk.keys))

    # data-quality checks: any failure stops the load
    df = validate_no_nulls(df, list(cfg.no_null_cols))
    for column, values in cfg.allowed_values.items():
        df = validate_column_values(df, column=column, values=values)
    for name in cfg.validators:
        df = VALIDATORS[name](df)
    df = validate_primary_keys(df, pk)

    write_to_silver(spark, df, cfg.silver_table, pk)
    logger.info("%s load complete.", cfg.silver_table)
    return {"silver_table": cfg.silver_table, "status": "loaded"}


def get_silver_config(name: str) -> SilverTableConfig:
    for cfg in SILVER_TABLES:
        if cfg.name == name:
            return cfg
    raise KeyError(f"No silver config named '{name}'")
