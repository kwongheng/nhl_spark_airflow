"""Static, data-only configuration.

This module replaces Fabric's ``dbo.config_datasets`` table and the per-notebook config
cells. It must stay free of heavy imports (no pyspark) because the DAG file imports it on
every scheduler parse.

Adding a dataset is a configuration change:
  1. add a ``DatasetConfig`` to ``DATASETS`` (landing + bronze), and
  2. add a ``SilverTableConfig`` to ``SILVER_TABLES`` (cleaning + validation).
The DAG builds its tasks and dependencies from these lists.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# --------------------------------------------------------------------------------------
# Storage layout (replaces the Fabric Lakehouse)
# --------------------------------------------------------------------------------------
# Inside the Astro container the project is mounted at /usr/local/airflow, so the default
# below lands on your host at <project>/include/lakehouse.
LAKEHOUSE_DIR = Path(os.getenv("NHL_LAKEHOUSE_DIR", "/usr/local/airflow/include/lakehouse"))
LANDING_DIR = LAKEHOUSE_DIR / "landing"  # Fabric: Files/landing
TABLES_DIR = LAKEHOUSE_DIR / "tables"  # Fabric: Tables/<schema>/<table>

SCHEMAS = ("bronze", "silver", "gold")

# Audit tables (Delta, created by the setup task)
FILE_MANIFEST_TABLE = "bronze._file_manifest"
SCHEMA_LOG_TABLE = "bronze._bronze_schema_log"


def table_path(table_name: str) -> str:
    """Map a schema-qualified name such as ``silver.game`` to its Delta folder."""
    schema, _, table = table_name.partition(".")
    if not table:
        raise ValueError(f"Expected '<schema>.<table>', got '{table_name}'")
    return str(TABLES_DIR / schema / table)


# --------------------------------------------------------------------------------------
# Landing + bronze  (Fabric: dbo.config_datasets)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class DatasetConfig:
    name: str  # bronze table is bronze.<name>
    kaggle_dataset: str
    source_file: str
    hash_columns: tuple[str, ...]  # columns that define row identity in bronze
    csv_options: dict[str, str] = field(default_factory=dict)
    is_active: bool = True

    @property
    def bronze_table(self) -> str:
        return f"bronze.{self.name}"

    @property
    def landing_path(self) -> str:
        return str(LANDING_DIR / self.source_file)


_KAGGLE = "martinellis/nhl-game-data"

DATASETS: tuple[DatasetConfig, ...] = (
    DatasetConfig("game", _KAGGLE, "game.csv", ("game_id", "season", "type")),
    DatasetConfig(
        "team_info",
        _KAGGLE,
        "team_info.csv",
        ("team_id", "franchiseId", "shortName", "teamName", "abbreviation"),
    ),
    DatasetConfig(
        "player_info",
        _KAGGLE,
        "player_info.csv",
        ("player_id", "firstName", "lastName", "birthDate", "primaryPosition", "nationality"),
    ),
    DatasetConfig(
        "game_goalie_stats",
        _KAGGLE,
        "game_goalie_stats.csv",
        ("game_id", "player_id", "team_id", "timeOnIce"),
    ),
    DatasetConfig(
        "game_skater_stats",
        _KAGGLE,
        "game_skater_stats.csv",
        ("game_id", "player_id", "team_id", "timeOnIce"),
    ),
    DatasetConfig(
        "game_teams_stats",
        _KAGGLE,
        "game_teams_stats.csv",
        ("game_id", "team_id", "HoA"),
    ),
)

DEFAULT_CSV_OPTIONS = {"header": "true", "inferSchema": "true"}


def active_datasets() -> list[DatasetConfig]:
    datasets = [d for d in DATASETS if d.is_active]
    if not datasets:
        raise ValueError("No active datasets configured in config.DATASETS.")
    return datasets


def kaggle_datasets() -> list[str]:
    """Distinct Kaggle datasets so each one is downloaded once."""
    return sorted({d.kaggle_dataset for d in active_datasets()})


# --------------------------------------------------------------------------------------
# Silver  (Fabric: the config cell at the top of each nb_03_* notebook)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class ForeignKey:
    """Rows without a match in ``ref_table`` (a silver table) are dropped.

    ``keys`` is either one column (same name on both sides) or two columns:
    (column in this table, column in the referenced table).
    """

    ref_table: str
    keys: tuple[str, ...]

    @property
    def ref_column(self) -> str:
        return self.keys[-1]


@dataclass(frozen=True)
class SilverTableConfig:
    name: str  # reads bronze.<name>, writes silver.<name>
    required_cols: tuple[str, ...]  # only these columns reach silver
    primary_keys: tuple[str, ...]
    no_null_cols: tuple[str, ...] = ()
    fill_null: dict[str, str] = field(default_factory=dict)
    date_cols: tuple[str, ...] = ()  # cast to DATE
    timestamp_cols: tuple[str, ...] = ()  # cast to TIMESTAMP
    # column -> Spark type. Uses TRY_CAST, so non-numeric text such as 'NA' becomes null
    # instead of failing (or leaving the whole column as a string).
    cast_types: dict[str, str] = field(default_factory=dict)
    allowed_values: dict[str, tuple[str, ...]] = field(default_factory=dict)
    foreign_keys: tuple[ForeignKey, ...] = ()
    validators: tuple[str, ...] = ()  # names registered in silver.VALIDATORS

    @property
    def bronze_table(self) -> str:
        return f"bronze.{self.name}"

    @property
    def silver_table(self) -> str:
        return f"silver.{self.name}"

    @property
    def upstream_silver(self) -> set[str]:
        return {fk.ref_table for fk in self.foreign_keys}


_TEAM_FK = ForeignKey("team_info", ("team_id",))
_GAME_FK = ForeignKey("game", ("game_id",))
_PLAYER_FK = ForeignKey("player_info", ("player_id",))
_GAME_PLAYER_KEYS = ("game_id", "player_id", "team_id")

SILVER_TABLES: tuple[SilverTableConfig, ...] = (
    SilverTableConfig(
        name="team_info",
        required_cols=("team_id", "franchiseId", "shortName", "teamName", "abbreviation"),
        primary_keys=("team_id",),
        no_null_cols=("team_id", "franchiseId", "shortName", "teamName", "abbreviation"),
    ),
    SilverTableConfig(
        name="player_info",
        required_cols=(
            "player_id",
            "firstName",
            "lastName",
            "birthDate",
            "primaryPosition",
            "nationality",
        ),
        primary_keys=("player_id",),
        no_null_cols=(
            "player_id",
            "firstName",
            "lastName",
            "birthDate",
            "primaryPosition",
            "nationality",
        ),
        fill_null={"nationality": "None"},
        date_cols=("birthDate",),
        allowed_values={"primaryPosition": ("LW", "D", "C", "RW", "G")},
    ),
    SilverTableConfig(
        name="game",
        required_cols=(
            "game_id",
            "season",
            "type",
            "date_time_GMT",
            "home_team_id",
            "away_team_id",
            "home_goals",
            "away_goals",
            "outcome",
        ),
        primary_keys=("game_id",),
        timestamp_cols=("date_time_GMT",),
        foreign_keys=(
            ForeignKey("team_info", ("home_team_id", "team_id")),
            ForeignKey("team_info", ("away_team_id", "team_id")),
        ),
        validators=("game_date_to_season",),
    ),
    SilverTableConfig(
        name="game_teams_stats",
        required_cols=(
            "game_id",
            "team_id",
            "HoA",
            "won",
            "settled_in",
            "goals",
            "shots",
            "hits",
            "pim",
            "powerPlayOpportunities",
            "powerPlayGoals",
            "faceOffWinPercentage",
            "giveaways",
            "takeaways",
            "blocked",
        ),
        primary_keys=("game_id", "team_id"),
        no_null_cols=("game_id", "team_id", "HoA", "won", "settled_in"),
        cast_types={
            "goals": "int",
            "shots": "int",
            "hits": "int",
            "pim": "int",
            "powerPlayOpportunities": "int",
            "powerPlayGoals": "int",
            "faceOffWinPercentage": "double",
            "giveaways": "int",
            "takeaways": "int",
            "blocked": "int",
        },
        allowed_values={
            "HoA": ("home", "away"),
            "settled_in": ("REG", "tbc", "OT"),
        },
        foreign_keys=(_GAME_FK, _TEAM_FK),
    ),
    SilverTableConfig(
        name="game_skater_stats",
        required_cols=(
            "game_id",
            "player_id",
            "team_id",
            "timeOnIce",
            "goals",
            "assists",
            "shots",
            "hits",
            "penaltyMinutes",
            "plusMinus",
            "powerPlayGoals",
            "powerPlayAssists",
            "faceOffWins",
            "faceoffTaken",
            "takeaways",
            "giveaways",
        ),
        primary_keys=_GAME_PLAYER_KEYS,
        no_null_cols=(
            "game_id",
            "player_id",
            "team_id",
            "timeOnIce",
            "goals",
            "assists",
            "shots",
            "penaltyMinutes",
            "plusMinus",
            "powerPlayGoals",
            "powerPlayAssists",
            "faceOffWins",
            "faceoffTaken",
        ),
        foreign_keys=(_GAME_FK, _TEAM_FK, _PLAYER_FK),
    ),
    SilverTableConfig(
        name="game_goalie_stats",
        required_cols=(
            "game_id",
            "player_id",
            "team_id",
            "timeOnIce",
            "shots",
            "saves",
            "decision",
        ),
        primary_keys=_GAME_PLAYER_KEYS,
        no_null_cols=("game_id", "player_id", "team_id", "timeOnIce", "shots", "saves"),
        foreign_keys=(_GAME_FK, _TEAM_FK, _PLAYER_FK),
    ),
)

# Silver tables the star-schema task reads (it runs after all of them).
STAR_INPUTS: tuple[str, ...] = (
    "team_info",
    "game",
    "game_teams_stats",
    "game_skater_stats",
    "game_goalie_stats",
)
