"""SparkSession factory with Delta Lake enabled (local mode, inside the Airflow container)."""
from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

from include.nhl_etl.config import LAKEHOUSE_DIR

logger = logging.getLogger(__name__)

# Keep modest: several Spark tasks can run in parallel inside one Docker container.
SPARK_MASTER = os.getenv("NHL_SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.getenv("NHL_SPARK_DRIVER_MEMORY", "2g")
SHUFFLE_PARTITIONS = os.getenv("NHL_SPARK_SHUFFLE_PARTITIONS", "8")


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.warehouse.dir", str(LAKEHOUSE_DIR / "warehouse"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Bronze/silver tolerate new source columns (drift is logged separately).
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.ui.enabled", "false")
    )

    try:
        # Adds the matching delta-spark jar to spark.jars.packages (resolved via Ivy).
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        logger.warning("delta-spark is not installed; assuming Delta jars are on the classpath.")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def spark_session(app_name: str) -> Iterator[SparkSession]:
    """Create a session for one Airflow task and always release the JVM afterwards."""
    spark = build_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()
