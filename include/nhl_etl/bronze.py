"""Landing -> bronze (Fabric: nb_02_landing_bronze_functions + second half of nb_02_landing_to_bronze).

Bronze is raw and append-only: the CSV is read as-is, technical columns are added, exact
duplicates are collapsed on a row hash, and only unseen hashes are inserted via a Delta
MERGE, so re-running is safe.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from include.nhl_etl.config import (
    DEFAULT_CSV_OPTIONS,
    FILE_MANIFEST_TABLE,
    SCHEMA_LOG_TABLE,
    DatasetConfig,
    table_path,
)
from include.nhl_etl.dbutils import table_exists

logger = logging.getLogger(__name__)

METADATA_COLS = {"_source_file", "_ingestion_timestamp", "_row_hash"}


# --------------------------------------------------------------------------------------
# Change detection
# --------------------------------------------------------------------------------------
def _sha256_of_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def has_file_content_changed(spark: SparkSession, landing_path: str) -> dict:
    """Compare the file's full-content SHA-256 with the manifest.

    Only content matters (not file name or modified time), so a re-upload of identical
    data counts as "no change". This only *checks*; call ``record_file_hash`` after a
    successful load so a failed ingestion is retried instead of skipped.
    """
    file_hash = _sha256_of_file(landing_path)
    already_seen = (
        spark.read.format("delta")
        .load(table_path(FILE_MANIFEST_TABLE))
        .filter((F.col("file_path") == landing_path) & (F.col("file_hash") == file_hash))
        .limit(1)
        .count()
        > 0
    )
    return {"changed": not already_seen, "file_hash": file_hash, "landing_path": landing_path}


def record_file_hash(spark: SparkSession, landing_path: str, file_hash: str) -> None:
    (
        spark.createDataFrame(
            [(landing_path, file_hash)], schema="file_path string, file_hash string"
        )
        .withColumn("detected_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .save(table_path(FILE_MANIFEST_TABLE))
    )


# --------------------------------------------------------------------------------------
# Schema drift
# --------------------------------------------------------------------------------------
def _log_schema_drift(
    spark: SparkSession,
    bronze_table: str,
    incoming_cols: list[str],
) -> None:
    """Record new/missing columns versus the existing bronze table in the schema log."""
    if not table_exists(spark, bronze_table):
        return  # first load, nothing to compare with

    existing = set(spark.read.format("delta").load(table_path(bronze_table)).columns) - METADATA_COLS
    incoming = set(incoming_cols)
    new_cols, dropped_cols = incoming - existing, existing - incoming
    if not new_cols and not dropped_cols:
        return

    (
        spark.createDataFrame(
            [
                (
                    datetime.now(timezone.utc).isoformat(),
                    bronze_table,
                    ",".join(sorted(new_cols)) or None,
                    ",".join(sorted(dropped_cols)) or None,
                )
            ],
            schema="detected_at string, table_name string, new_columns string, dropped_columns string",
        )
        .write.format("delta")
        .mode("append")
        .save(table_path(SCHEMA_LOG_TABLE))
    )

    if new_cols:
        logger.warning("[%s][SCHEMA DRIFT] New columns: %s", bronze_table, new_cols)
    if dropped_cols:
        logger.warning("[%s][SCHEMA DRIFT] Columns missing from this load: %s", bronze_table, dropped_cols)


# --------------------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------------------
def ingest_to_bronze(
    spark: SparkSession,
    landing_path: str,
    bronze_table: str,
    hash_columns: tuple[str, ...] | list[str],
    csv_options: dict[str, str] | None = None,
) -> dict:
    """Idempotent, insert-only CSV -> bronze Delta load.

    Detects schema drift, adds ``_source_file`` / ``_ingestion_timestamp`` / ``_row_hash``,
    collapses duplicates within the batch, and inserts only rows whose hash is new.
    """
    csv_options = csv_options or DEFAULT_CSV_OPTIONS
    try:
        reader = spark.read
        for k, v in csv_options.items():
            reader = reader.option(k, v)
        raw_df = reader.csv(landing_path)
        rows_read = raw_df.count()
        logger.info("[%s] Rows read from '%s': %s", bronze_table, landing_path, rows_read)

        _log_schema_drift(spark, bronze_table, raw_df.columns)

        missing = [c for c in hash_columns if c not in raw_df.columns]
        if missing:
            raise ValueError(
                f"[{bronze_table}] Expected hash columns not present in this load: {missing}"
            )

        df = (
            raw_df.withColumn("_source_file", F.input_file_name())
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn(
                "_row_hash",
                F.sha2(
                    F.concat_ws("||", *[F.col(c).cast(StringType()) for c in hash_columns]),
                    256,
                ),
            )
            .dropDuplicates(["_row_hash"])
            .cache()
        )
        rows_after_dedup = df.count()
        rows_deduped = rows_read - rows_after_dedup
        if rows_deduped > 0:
            logger.info(
                "[%s][DEDUP] Collapsed %s duplicate row(s) in this batch (%s -> %s).",
                bronze_table, rows_deduped, rows_read, rows_after_dedup,
            )

        path = table_path(bronze_table)
        if not table_exists(spark, bronze_table):
            df.write.format("delta").save(path)
            rows_inserted = total_rows_now = rows_after_dedup
            logger.info("[%s] Created table with %s row(s).", bronze_table, rows_inserted)
        else:
            before_count = spark.read.format("delta").load(path).count()
            (
                DeltaTable.forPath(spark, path)
                .alias("t")
                .merge(df.alias("s"), "t._row_hash = s._row_hash")
                .whenNotMatchedInsertAll()
                .execute()
            )
            total_rows_now = spark.read.format("delta").load(path).count()
            rows_inserted = total_rows_now - before_count
            logger.info(
                "[%s] Merge complete. Inserted %s new row(s). Total rows now: %s.",
                bronze_table, rows_inserted, total_rows_now,
            )

        df.unpersist()
        return {
            "bronze_table": bronze_table,
            "rows_read": rows_read,
            "rows_deduped": rows_deduped,
            "rows_inserted": rows_inserted,
            "total_rows_now": total_rows_now,
        }
    except Exception:
        logger.exception("[%s] Ingestion from '%s' failed", bronze_table, landing_path)
        raise


def run_bronze(spark: SparkSession, cfg: DatasetConfig) -> dict:
    """Load one landed file into its bronze table, skipping unchanged files."""
    logger.info("Loading %s -> %s", cfg.source_file, cfg.bronze_table)
    check = has_file_content_changed(spark, cfg.landing_path)
    if not check["changed"]:
        logger.info("Skipping (no change): %s", cfg.landing_path)
        return {"bronze_table": cfg.bronze_table, "status": "skipped"}

    metrics = ingest_to_bronze(
        spark,
        cfg.landing_path,
        cfg.bronze_table,
        hash_columns=cfg.hash_columns,
        csv_options=cfg.csv_options or None,
    )
    record_file_hash(spark, cfg.landing_path, check["file_hash"])
    return {**metrics, "status": "loaded"}
