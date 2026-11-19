"""Kaggle -> landing zone (Fabric: first half of nb_02_landing_to_bronze)."""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from include.nhl_etl.config import LANDING_DIR, DatasetConfig, active_datasets

logger = logging.getLogger(__name__)


def download_kaggle_dataset(kaggle_dataset: str) -> Path:
    """Download via kagglehub (credentials: KAGGLE_USERNAME/KAGGLE_KEY env vars or kaggle.json)."""
    import kagglehub  # imported lazily: only the landing task needs it

    try:
        download_path = Path(kagglehub.dataset_download(kaggle_dataset))
    except Exception as e:
        raise RuntimeError(
            f"Failed to download Kaggle dataset '{kaggle_dataset}'. Check that Kaggle "
            f"credentials are configured for this environment. Original error: {e}"
        ) from e

    if not download_path.is_dir():
        raise RuntimeError(f"kagglehub reported success but '{download_path}' does not exist.")
    return download_path


def _build_file_lookup(download_path: Path) -> dict[str, Path]:
    return {p.name: p for p in download_path.rglob("*") if p.is_file()}


def _has_data_rows(path: Path) -> bool:
    """True if the CSV has a header plus at least one data row."""
    with path.open("rb") as f:
        return sum(1 for _ in zip(range(2), f)) >= 2


def land_kaggle_dataset(kaggle_dataset: str) -> list[str]:
    """Download one Kaggle dataset and copy the configured files into the landing zone.

    Raises if any configured file is missing or empty so Airflow can retry/alert,
    instead of letting bronze run on a partial landing.
    """
    configs: list[DatasetConfig] = [
        d for d in active_datasets() if d.kaggle_dataset == kaggle_dataset
    ]
    download_path = download_kaggle_dataset(kaggle_dataset)
    logger.info("Dataset %s downloaded to %s", kaggle_dataset, download_path)
    available = _build_file_lookup(download_path)

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    landed: list[str] = []
    failed: list[str] = []

    for cfg in configs:
        source = available.get(cfg.source_file)
        if source is None:
            logger.error("%s not found in %s (table %s)", cfg.source_file, kaggle_dataset, cfg.bronze_table)
            failed.append(cfg.bronze_table)
            continue
        if not _has_data_rows(source):
            logger.error("%s is empty (table %s)", cfg.source_file, cfg.bronze_table)
            failed.append(cfg.bronze_table)
            continue

        # copy to a temp name then rename, so a crash never leaves a half-written file
        target = LANDING_DIR / cfg.source_file
        tmp = target.with_suffix(target.suffix + ".tmp")
        shutil.copyfile(source, tmp)
        os.replace(tmp, target)
        logger.info("Landed %s -> %s", cfg.source_file, target)
        landed.append(cfg.source_file)

    if failed:
        raise RuntimeError(f"{len(failed)} table(s) failed to land: {failed}")
    return landed
