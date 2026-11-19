"""Shared Spark/Delta helpers (Fabric: nb_00_dbutils).

Tables are addressed by schema-qualified names (``silver.game``) that map to Delta
folders via ``config.table_path``. Path-based access needs no metastore, so parallel
Airflow tasks (separate Spark sessions) all see the same tables.
"""
from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

from include.nhl_etl.config import table_path

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# Table access
# --------------------------------------------------------------------------------------
def table_exists(spark: SparkSession, table_name: str) -> bool:
    return DeltaTable.isDeltaTable(spark, table_path(table_name))


def load_table(
    spark: SparkSession,
    table_name: str,
    columns: list[str] | None = None,
) -> DataFrame:
    """Load a Delta table, optionally selecting a subset of columns."""
    df = spark.read.format("delta").load(table_path(table_name))
    logger.info("Loaded %s", table_name)
    return df.select(*columns) if columns else df


def write_table(df: DataFrame, table_name: str) -> None:
    """Overwrite a Delta table (data and schema)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path(table_name))
    )
    logger.info("Wrote %s", table_name)


# --------------------------------------------------------------------------------------
# Incremental read + de-duplication
# --------------------------------------------------------------------------------------
def read_new_bronze_rows(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    columns: list[str] | None = None,
    watermark_col: str = "_bronze_ingested_at",
) -> DataFrame:
    """Read bronze rows newer than the latest bronze timestamp already in silver.

    On the first run (no silver table, or no watermark yet) the whole bronze table is read.
    """
    df = load_table(spark, bronze_table)
    if columns:
        # dict.fromkeys removes duplicates if the metadata columns were already requested
        df = df.select(*dict.fromkeys([*columns, "_ingestion_timestamp", "_row_hash"]))

    if table_exists(spark, silver_table):
        last_seen = load_table(spark, silver_table).agg(F.max(watermark_col)).first()[0]
        if last_seen is not None:
            logger.info("[%s] Reading rows after watermark %s", bronze_table, last_seen)
            return df.filter(F.col("_ingestion_timestamp") > last_seen)

    logger.info("[%s] No watermark found, reading full table", bronze_table)
    return df


def keep_latest_per_key(df: DataFrame, primary_keys: list[str]) -> DataFrame:
    """Keep the newest row (by ``_ingestion_timestamp``) per business key.

    Renames ``_ingestion_timestamp`` to ``_bronze_ingested_at`` for silver.
    """
    order: list[Column] = [F.col("_ingestion_timestamp").desc()]
    if "_row_hash" in df.columns:
        order.append(F.col("_row_hash").asc())  # deterministic tie-break

    w: WindowSpec = Window.partitionBy(*primary_keys).orderBy(*order)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumnRenamed("_ingestion_timestamp", "_bronze_ingested_at")
    )


# --------------------------------------------------------------------------------------
# Data-quality checks (raise to stop the load)
# --------------------------------------------------------------------------------------
def validate_no_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    if not columns:
        return df
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in columns]
    ).first()
    for column_name in columns:
        if (null_count := null_counts[column_name]) > 0:
            raise RuntimeError(f"Column '{column_name}' contains {null_count} null value(s).")
    return df


def validate_column_values(df: DataFrame, column: str, values: tuple[str, ...]) -> DataFrame:
    """Column must be non-null and within the allowed set."""
    invalid_count = df.filter(F.col(column).isNull() | (~F.col(column).isin(*values))).count()
    if invalid_count > 0:
        raise RuntimeError(f"Column '{column}' contains {invalid_count} invalid or null value(s).")
    return df


def validate_primary_keys(df: DataFrame, keys: list[str]) -> DataFrame:
    duplicate_count = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    if duplicate_count > 0:
        raise RuntimeError(
            f"Primary key violation: found {duplicate_count} duplicate key value(s) for {keys}."
        )
    return df


def drop_foreign_key_violations(
    df: DataFrame,
    ftable: DataFrame,
    keys: list[str],
) -> DataFrame:
    """Keep only rows whose foreign key exists in the referenced table (left semi join).

    ``keys`` is one column (same name both sides) or (column in df, column in ftable).
    """
    if len(keys) == 1:
        return df.join(ftable, on=keys[0], how="left_semi")
    if len(keys) == 2:
        return df.join(ftable, df[keys[0]] == ftable[keys[1]], how="left_semi")
    raise RuntimeError(f"Expected 1 or 2 key values, received {len(keys)}: {keys}")


# --------------------------------------------------------------------------------------
# Silver upsert (SCD Type 1)
# --------------------------------------------------------------------------------------
def write_to_silver(
    spark: SparkSession,
    df: DataFrame,
    silver_table: str,
    primary_keys: list[str],
) -> None:
    """Upsert into silver: one row per business key, newest bronze record wins."""
    try:
        if df.isEmpty():
            logger.info("[%s] No rows to write, skipping.", silver_table)
            return

        path = table_path(silver_table)
        if not DeltaTable.isDeltaTable(spark, path):
            df.write.format("delta").save(path)
            logger.info("[%s] Created table.", silver_table)
            return

        merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in primary_keys)
        (
            DeltaTable.forPath(spark, path)
            .alias("t")
            .merge(df.alias("s"), merge_cond)
            .whenMatchedUpdateAll(
                condition="t._bronze_ingested_at IS NULL "
                "OR s._bronze_ingested_at > t._bronze_ingested_at"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("[%s] Merge complete.", silver_table)
    except Exception:
        logger.exception("[%s] Silver write failed", silver_table)
        raise
