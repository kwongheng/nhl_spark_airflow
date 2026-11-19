"""Silver star schema (Fabric: nb_03_4_teams_players_star_silver -> ``games_teams_star``).

Builds dim_team, fact_team_game, fact_team_champion, fact_skater_game, fact_goalie_game
from the cleaned silver tables.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

from include.nhl_etl.dbutils import load_table, write_table

logger = logging.getLogger(__name__)

GAME_TABLE = "silver.game"
GAME_TEAMS_STATS_TABLE = "silver.game_teams_stats"
GAME_SKATER_STATS_TABLE = "silver.game_skater_stats"
GAME_GOALIE_STATS_TABLE = "silver.game_goalie_stats"
TEAM_INFO_TABLE = "silver.team_info"

DIM_TEAM_TABLE = "silver.dim_team"
FACT_TEAM_GAME_TABLE = "silver.fact_team_game"
FACT_TEAM_CHAMPION_TABLE = "silver.fact_team_champion"
FACT_SKATER_GAME_TABLE = "silver.fact_skater_game"
FACT_GOALIE_GAME_TABLE = "silver.fact_goalie_game"


def build_dim_team(team_info: DataFrame) -> DataFrame:
    return team_info.select(
        "team_id",
        "franchiseId",
        "shortName",
        "teamName",
        "abbreviation",
        F.concat_ws(" ", "shortName", "teamName").alias("full_name"),
    )


def build_fact_team_game(game: DataFrame, game_teams_stats: DataFrame) -> DataFrame:
    """One row per team per game with the opponent's stats alongside.

    Points follow the NHL system: win = 2, OT/SO loss = 1, regulation loss = 0.
    Pre-2005-06 ties appear as won=FALSE, settled_in='OT' on both sides (1 point each).
    A few settled_in='tbc' rows (~0.1%, a source data-quality artifact) get 0 points.
    """
    team_won_stats = game_teams_stats.withColumn(
        "won_flag",
        F.when(F.col("won").cast("string").isin("TRUE", "true", "1"), F.lit(1)).otherwise(F.lit(0)),
    )

    return (
        game.alias("g")
        .join(team_won_stats.alias("a"), F.col("g.game_id") == F.col("a.game_id"))
        .join(
            team_won_stats.alias("b"),
            (F.col("g.game_id") == F.col("b.game_id")) & (F.col("a.team_id") != F.col("b.team_id")),
        )
        .select(
            F.col("g.game_id"),
            F.col("g.season"),
            F.col("g.season_start_year"),
            F.col("g.type"),
            F.col("a.team_id"),
            F.col("a.HoA"),
            F.col("a.won_flag").alias("won"),
            F.col("a.settled_in"),
            F.col("a.goals"),
            F.col("a.shots"),
            F.col("a.hits"),
            F.col("a.pim"),
            F.col("a.powerPlayOpportunities"),
            F.col("a.powerPlayGoals"),
            F.col("a.faceOffWinPercentage"),
            F.col("a.giveaways"),
            F.col("a.takeaways"),
            F.col("a.blocked"),
            F.col("b.team_id").alias("opp_team_id"),
            F.col("b.goals").alias("opp_goals"),
            F.col("b.shots").alias("opp_shots"),
            F.col("b.powerPlayOpportunities").alias("opp_powerPlayOpportunities"),
            F.col("b.powerPlayGoals").alias("opp_powerPlayGoals"),
            F.when(F.col("a.won_flag") == 1, F.lit(2))
            .when(F.col("a.settled_in") == "OT", F.lit(1))
            .otherwise(F.lit(0))
            .alias("points"),
        )
    )


def build_fact_team_champion(game: DataFrame) -> DataFrame:
    """Champion = winner of each season's last playoff game.

    Tied-score rows are excluded first so a game that went to OT is not picked up as the
    season's final state.
    """
    playoff_games = game.filter(
        (F.col("type") == "P") & (F.col("home_goals") != F.col("away_goals"))
    ).select(
        "season",
        "game_id",
        "home_team_id",
        "away_team_id",
        "home_goals",
        "away_goals",
        "date_time_GMT",
        "season_start_year",
    )

    w_last: WindowSpec = Window.partitionBy("season").orderBy(F.col("date_time_GMT").desc())
    return (
        playoff_games.withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .select(
            "season",
            "season_start_year",
            F.when(F.col("home_goals") > F.col("away_goals"), F.col("home_team_id"))
            .otherwise(F.col("away_team_id"))
            .alias("champion_team_id"),
        )
    )


def build_fact_skater_game(game: DataFrame, game_skater_stats: DataFrame) -> DataFrame:
    return (
        game_skater_stats.alias("s")
        .join(game.alias("g"), F.col("s.game_id") == F.col("g.game_id"))
        .select(
            "s.game_id",
            "s.player_id",
            "s.team_id",
            "g.season",
            "g.season_start_year",
            "g.type",
            "s.timeOnIce",
            "s.goals",
            "s.assists",
            (F.col("s.goals") + F.col("s.assists")).alias("points"),
            "s.shots",
            "s.hits",
            "s.penaltyMinutes",
            "s.plusMinus",
            "s.powerPlayGoals",
            "s.powerPlayAssists",
            "s.faceOffWins",
            "s.faceoffTaken",
            "s.takeaways",
            "s.giveaways",
        )
    )


def build_fact_goalie_game(game: DataFrame, game_goalie_stats: DataFrame) -> DataFrame:
    return (
        game_goalie_stats.alias("gs")
        .join(game.alias("g"), F.col("gs.game_id") == F.col("g.game_id"))
        .select(
            "gs.game_id",
            "gs.player_id",
            "gs.team_id",
            "g.season",
            "g.season_start_year",
            "g.type",
            "gs.timeOnIce",
            "gs.shots",
            "gs.saves",
            (F.col("gs.shots") - F.col("gs.saves")).alias("goals_against"),
            "gs.decision",
        )
    )


def run_star(spark: SparkSession) -> dict:
    """Build and write all star-schema tables."""
    game = load_table(spark, GAME_TABLE).withColumn(
        "season_start_year", F.substring("season", 1, 4).cast("int")
    )
    game_teams_stats = load_table(spark, GAME_TEAMS_STATS_TABLE)
    game_skater_stats = load_table(spark, GAME_SKATER_STATS_TABLE)
    game_goalie_stats = load_table(spark, GAME_GOALIE_STATS_TABLE)
    team_info = load_table(spark, TEAM_INFO_TABLE)

    write_table(build_dim_team(team_info), DIM_TEAM_TABLE)
    write_table(build_fact_team_game(game, game_teams_stats), FACT_TEAM_GAME_TABLE)
    write_table(build_fact_team_champion(game), FACT_TEAM_CHAMPION_TABLE)
    write_table(build_fact_skater_game(game, game_skater_stats), FACT_SKATER_GAME_TABLE)
    write_table(build_fact_goalie_game(game, game_goalie_stats), FACT_GOALIE_GAME_TABLE)

    return {
        "tables": [
            DIM_TEAM_TABLE,
            FACT_TEAM_GAME_TABLE,
            FACT_TEAM_CHAMPION_TABLE,
            FACT_SKATER_GAME_TABLE,
            FACT_GOALIE_GAME_TABLE,
        ]
    }
