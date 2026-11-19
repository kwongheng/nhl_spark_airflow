"""One-time/idempotent setup (Fabric: nb_00_setup_config).

Creates the folder layout and the two bronze audit tables. Creating the audit tables up
front means the parallel bronze tasks never race to create them.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from include.nhl_etl.config import (
    FILE_MANIFEST_TABLE,
    LAKEHOUSE_DIR,
    LANDING_DIR,
    SCHEMA_LOG_TABLE,
    SCHEMAS,
    TABLES_DIR,
    table_path,
)
from include.nhl_etl.dbutils import table_exists

logger = logging.getLogger(__name__)

_MANIFEST_SCHEMA = StructType(
    [
        StructField("file_path", StringType(), False),
        StructField("file_hash", StringType(), False),
        StructField("detected_at", TimestampType(), True),
    ]
)

_SCHEMA_LOG_SCHEMA = StructType(
    [
        StructField("detected_at", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("new_columns", StringType(), True),
        StructField("dropped_columns", StringType(), True),
    ]
)


def setup_lakehouse(spark: SparkSession) -> None:
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    (LAKEHOUSE_DIR / "warehouse").mkdir(parents=True, exist_ok=True)
    for schema in SCHEMAS:
        (TABLES_DIR / schema).mkdir(parents=True, exist_ok=True)

    for table_name, schema in (
        (FILE_MANIFEST_TABLE, _MANIFEST_SCHEMA),
        (SCHEMA_LOG_TABLE, _SCHEMA_LOG_SCHEMA),
    ):
        if not table_exists(spark, table_name):
            spark.createDataFrame([], schema).write.format("delta").save(table_path(table_name))
            logger.info("Created audit table %s", table_name)
