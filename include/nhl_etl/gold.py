"""Gold layer (Fabric: nb_04_teams_players_forecast_gold -> ``teams_players_forecast``).

Two steps so each can be retried on its own:
  1. ``run_season_summaries``: team / skater / goalie season rollups
  2. ``run_forecasts``: recency-weighted projections, backtest, 2020-21 odds
"""
from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window, WindowSpec

from include.nhl_etl.dbutils import load_table, write_table

logger = logging.getLogger(__name__)

# Silver inputs
DIM_TEAM_TABLE = "silver.dim_team"
FACT_TEAM_GAME_TABLE = "silver.fact_team_game"
FACT_TEAM_CHAMPION_TABLE = "silver.fact_team_champion"
FACT_SKATER_GAME_TABLE = "silver.fact_skater_game"
FACT_GOALIE_GAME_TABLE = "silver.fact_goalie_game"
PLAYER_INFO_TABLE = "silver.player_info"

# Gold outputs
TEAM_SEASON_TABLE = "gold.team_season"
SKATER_SEASON_TABLE = "gold.skater_season"
GOALIE_SEASON_TABLE = "gold.goalie_season"
CHAMPION_VALIDATION_HISTORY_TABLE = "gold.champion_validation_history"
SKATER_VALIDATION_HISTORY_TABLE = "gold.skater_validation_history"
TEAM_PROJECTION_2021_RAW_TABLE = "gold.team_projection_2021_raw"
CHAMPIONSHIP_ODDS_2021_TABLE = "gold.championship_odds_2021"
BEST_PLAYER_FORECAST_2021_TABLE = "gold.best_player_forecast_2021"
ACTUAL_2020_21_RESULTS_TABLE = "gold.actual_2020_21_results"

# Forecast settings
# The source has no playoff games before 2010-11, so fact_team_champion has no real outcome
# for as-of years before 2009. Backtest 2009..2018 (i.e. 2018-19 -> 2019-20 is the last pair).
BACKTEST_YEARS = range(2009, 2019)
FORECAST_AS_OF_YEAR = 2019  # "everything through 2019-20" -> predict 2020-21
SOFTMAX_K = 15.0  # higher = more top-heavy odds, lower = flatter


def safe_div(numer: Column, denom: Column | float) -> Column:
    """Division that returns null instead of erroring when the denominator is 0."""
    return F.when(denom == 0, F.lit(None)).otherwise(numer / denom)


# --------------------------------------------------------------------------------------
# Season summaries (regular season only)
# --------------------------------------------------------------------------------------
def build_team_season(
    fact_team_game: DataFrame,
    fact_team_champion: DataFrame,
    dim_team: DataFrame,
) -> DataFrame:
    agg = (
        fact_team_game.filter(F.col("type") == "R")
        .groupBy("team_id", "season", "season_start_year")
        .agg(
            F.count("*").alias("games_played"),
            F.sum("won").alias("wins"),
            F.sum(F.when((F.col("won") == 0) & (F.col("settled_in") == "REG"), 1).otherwise(0)).alias("reg_losses"),
            F.sum(F.when((F.col("won") == 0) & (F.col("settled_in") == "OT"), 1).otherwise(0)).alias("ot_so_losses_or_ties"),
            F.sum("points").alias("points"),
            F.sum("goals").alias("goals_for"),
            F.sum("opp_goals").alias("goals_against"),
            F.sum("shots").alias("shots_for"),
            F.sum("opp_shots").alias("shots_against"),
            F.sum("powerPlayGoals").alias("pp_goals"),
            F.sum("powerPlayOpportunities").alias("pp_opportunities"),
            F.sum("opp_powerPlayGoals").alias("opp_pp_goals"),
            F.sum("opp_powerPlayOpportunities").alias("opp_pp_opportunities"),
            F.avg("faceOffWinPercentage").alias("avg_faceoff_pct"),
            F.sum(F.col("giveaways") - F.col("takeaways")).alias("give_take_diff_total"),
        )
        .withColumn("points_pct", safe_div(F.col("points"), 2.0 * F.col("games_played")))
        .withColumn("goal_diff", F.col("goals_for") - F.col("goals_against"))
        .withColumn("goals_for_per_game", safe_div(F.col("goals_for"), F.col("games_played")))
        .withColumn("goals_against_per_game", safe_div(F.col("goals_against"), F.col("games_played")))
        .withColumn("shots_for_per_game", safe_div(F.col("shots_for"), F.col("games_played")))
        .withColumn("shots_against_per_game", safe_div(F.col("shots_against"), F.col("games_played")))
        .withColumn("pp_pct", safe_div(F.col("pp_goals"), F.col("pp_opportunities")) * 100)
        .withColumn("pk_pct", F.lit(100) - safe_div(F.col("opp_pp_goals"), F.col("opp_pp_opportunities")) * 100)
        .withColumn("giveaway_takeaway_diff_per_game", safe_div(F.col("give_take_diff_total"), F.col("games_played")))
    )

    return (
        agg.join(dim_team, on="team_id", how="left")
        .join(fact_team_champion.select("season", "champion_team_id"), on="season", how="left")
        .withColumn("is_champion", F.when(F.col("champion_team_id") == F.col("team_id"), 1).otherwise(0))
    )


def _primary_team(fact: DataFrame) -> DataFrame:
    """Team a player appeared for most in a season (resolves mid-season trades)."""
    counts = (
        fact.filter(F.col("type") == "R")
        .groupBy("player_id", "season", "team_id")
        .agg(F.count("*").alias("g"))
    )
    w: WindowSpec = Window.partitionBy("player_id", "season").orderBy(F.col("g").desc())
    return (
        counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("player_id", "season", F.col("team_id").alias("primary_team_id"))
    )


def build_skater_season(fact_skater_game: DataFrame, player_info: DataFrame) -> DataFrame:
    agg = (
        fact_skater_game.filter(F.col("type") == "R")
        .groupBy("player_id", "season", "season_start_year")
        .agg(
            F.count("*").alias("games_played"),
            F.sum("goals").alias("goals"),
            F.sum("assists").alias("assists"),
            F.sum("points").alias("points"),
            F.sum("shots").alias("shots"),
            F.sum("hits").alias("hits"),
            F.sum("penaltyMinutes").alias("pim"),
            F.sum("plusMinus").alias("plus_minus"),
            F.sum("powerPlayGoals").alias("pp_goals"),
            F.sum("powerPlayAssists").alias("pp_assists"),
            F.sum("timeOnIce").alias("total_toi_seconds"),
        )
    )
    return (
        agg.join(_primary_team(fact_skater_game), on=["player_id", "season"], how="left")
        .join(player_info, on="player_id", how="left")
        .withColumn("approx_age", F.col("season_start_year") - F.year("birthDate"))
        .withColumn("points_per_game", safe_div(F.col("points"), F.col("games_played")))
        .withColumn("avg_toi_minutes_per_game", safe_div(F.col("total_toi_seconds"), F.col("games_played")) / 60.0)
        .withColumn("points_per_60", safe_div(F.col("points") * 3600.0, F.col("total_toi_seconds")))
    )


def build_goalie_season(fact_goalie_game: DataFrame, player_info: DataFrame) -> DataFrame:
    agg = (
        fact_goalie_game.filter(F.col("type") == "R")
        .groupBy("player_id", "season", "season_start_year")
        .agg(
            F.count("*").alias("games_played"),
            F.sum(F.when(F.col("decision") == "W", 1).otherwise(0)).alias("wins"),
            F.sum(F.when(F.col("decision") == "L", 1).otherwise(0)).alias("losses"),
            F.sum("shots").alias("shots_against"),
            F.sum("saves").alias("saves"),
            F.sum("goals_against").alias("goals_against"),
            F.sum("timeOnIce").alias("total_toi_seconds"),
        )
    )
    return (
        agg.join(_primary_team(fact_goalie_game), on=["player_id", "season"], how="left")
        .join(player_info, on="player_id", how="left")
        .withColumn("approx_age", F.col("season_start_year") - F.year("birthDate"))
        .withColumn("save_pct", safe_div(F.col("saves"), F.col("shots_against")))
        .withColumn("goals_against_avg_per60", safe_div(F.col("goals_against") * 3600.0, F.col("total_toi_seconds")))
        .withColumn("total_toi_hours", F.col("total_toi_seconds") / 3600.0)
    )


def run_season_summaries(spark: SparkSession) -> dict:
    dim_team = load_table(spark, DIM_TEAM_TABLE)
    fact_team_game = load_table(spark, FACT_TEAM_GAME_TABLE)
    fact_team_champion = load_table(spark, FACT_TEAM_CHAMPION_TABLE)
    fact_skater_game = load_table(spark, FACT_SKATER_GAME_TABLE)
    fact_goalie_game = load_table(spark, FACT_GOALIE_GAME_TABLE)
    player_info = load_table(spark, PLAYER_INFO_TABLE)

    write_table(build_team_season(fact_team_game, fact_team_champion, dim_team), TEAM_SEASON_TABLE)
    write_table(build_skater_season(fact_skater_game, player_info), SKATER_SEASON_TABLE)
    write_table(build_goalie_season(fact_goalie_game, player_info), GOALIE_SEASON_TABLE)
    return {"tables": [TEAM_SEASON_TABLE, SKATER_SEASON_TABLE, GOALIE_SEASON_TABLE]}


# --------------------------------------------------------------------------------------
# Forecasting
# --------------------------------------------------------------------------------------
def project_champion_asof(
    as_of_year: int,
    *,
    dim_team: DataFrame,
    fact_team_champion: DataFrame,
    team_season: DataFrame,
) -> DataFrame:
    """Project the likely champion for the season after ``as_of_year``.

    Uses a recency-weighted average (weights 3/2/1, newest first) of each team's last three
    seasons up to and including ``as_of_year``; teams need at least two such seasons. The
    next season's real champion is attached when known, so the same code serves both the
    backtest and the real forecast.
    """
    team_w: WindowSpec = Window.partitionBy("team_id").orderBy(F.col("season_start_year").desc())

    scored = (
        team_season.filter(F.col("season_start_year") <= as_of_year)
        .withColumn("season_rn", F.row_number().over(team_w))
        .filter(F.col("season_rn") <= 3)
        .withColumn("wt", 4 - F.col("season_rn"))
        .groupBy("team_id")
        .agg(
            (F.sum(F.col("points_pct") * F.col("wt")) / F.sum("wt")).alias("proj_points_pct"),
            (F.sum(F.col("goal_diff") * F.col("wt")) / F.sum("wt")).alias("proj_goal_diff"),
            F.count("*").alias("seasons_used"),
        )
        .filter(F.col("seasons_used") >= 2)
        .withColumn("projected_rank", F.rank().over(Window.orderBy(F.col("proj_points_pct").desc())))
    )

    champ_row: Row | None = (
        fact_team_champion.filter(F.col("season_start_year") == as_of_year + 1)
        .select("champion_team_id")
        .first()
    )
    champ_id = champ_row["champion_team_id"] if champ_row is not None else None

    return (
        scored.join(dim_team, on="team_id", how="left")
        .withColumn("as_of_season_start_year", F.lit(as_of_year))
        .withColumn("champion_team_id", F.lit(champ_id))
        .withColumn("actually_won_next_season", F.when(F.col("team_id") == F.lit(champ_id), 1).otherwise(0))
        .select(
            "as_of_season_start_year",
            "team_id",
            "teamName",
            "abbreviation",
            "proj_points_pct",
            "proj_goal_diff",
            "seasons_used",
            "projected_rank",
            "champion_team_id",
            "actually_won_next_season",
        )
    )


def project_top_scorer_asof(
    as_of_year: int,
    *,
    dim_team: DataFrame,
    skater_season: DataFrame,
) -> DataFrame:
    """Project the league's top scorers for the season after ``as_of_year``.

    Recency-weighted (3/2/1) points per game over a player's last three seasons with at
    least 20 games each; two qualifying seasons required. Actual next-season points are
    attached when available for evaluation.
    """
    player_w: WindowSpec = Window.partitionBy("player_id").orderBy(F.col("season_start_year").desc())

    scored = (
        skater_season.filter((F.col("season_start_year") <= as_of_year) & (F.col("games_played") >= 20))
        .withColumn("season_rn", F.row_number().over(player_w))
        .filter(F.col("season_rn") <= 3)
        .withColumn("wt", 4 - F.col("season_rn"))
        .groupBy("player_id")
        .agg(
            F.max("firstName").alias("firstName"),
            F.max("lastName").alias("lastName"),
            F.max("primaryPosition").alias("primaryPosition"),
            F.max("primary_team_id").alias("primary_team_id"),
            (F.sum(F.col("points_per_game") * F.col("wt")) / F.sum("wt")).alias("proj_points_per_game"),
            F.count("*").alias("seasons_used"),
        )
        .filter(F.col("seasons_used") >= 2)
        .withColumn("proj_points_82gp", F.col("proj_points_per_game") * 82.0)
        .withColumn("projected_rank", F.rank().over(Window.orderBy(F.col("proj_points_82gp").desc())))
    )

    actual_next = skater_season.filter(F.col("season_start_year") == as_of_year + 1).select(
        "player_id", F.col("points").alias("actual_next_season_points")
    )

    return (
        scored.join(dim_team.withColumnRenamed("team_id", "primary_team_id"), on="primary_team_id", how="left")
        .join(actual_next, on="player_id", how="left")
        .withColumn("as_of_season_start_year", F.lit(as_of_year))
        .select(
            "as_of_season_start_year",
            "player_id",
            "firstName",
            "lastName",
            "primaryPosition",
            "teamName",
            "proj_points_per_game",
            "proj_points_82gp",
            "seasons_used",
            "projected_rank",
            "actual_next_season_points",
        )
    )


def build_championship_odds(team_projection: DataFrame, k: float = SOFTMAX_K) -> DataFrame:
    """Turn projected points% into normalised 'betting-style' probabilities via softmax."""
    scored = team_projection.withColumn("raw_score", F.exp(F.col("proj_points_pct") * k))
    total_score = scored.agg(F.sum("raw_score").alias("t")).collect()[0]["t"]
    return scored.withColumn(
        "championship_probability", F.col("raw_score") / F.lit(total_score)
    ).select(
        "team_id",
        "teamName",
        "abbreviation",
        "proj_points_pct",
        "projected_rank",
        "championship_probability",
    )


def build_actual_2020_21_results(spark: SparkSession) -> DataFrame:
    """Hard-coded real-world 2020-21 outcomes (not in the dataset) to grade the forecast."""
    return spark.createDataFrame(
        [
            ("Hart Trophy (MVP)", "Connor McDavid", "Edmonton Oilers", "105 points (33G, 72A) in a 56-game season"),
            ("Art Ross Trophy (points leader)", "Connor McDavid", "Edmonton Oilers", "105 points, led the NHL in scoring"),
            ("Stanley Cup Champion", None, "Tampa Bay Lightning", "Beat the Montreal Canadiens in the Final — back-to-back champions"),
        ],
        ["category", "winner_name", "team", "detail"],
    )


def run_forecasts(spark: SparkSession) -> dict:
    dim_team = load_table(spark, DIM_TEAM_TABLE)
    fact_team_champion = load_table(spark, FACT_TEAM_CHAMPION_TABLE)
    team_season = load_table(spark, TEAM_SEASON_TABLE)
    skater_season = load_table(spark, SKATER_SEASON_TABLE)

    # Backtest
    champion_history = reduce(
        DataFrame.unionByName,
        [
            project_champion_asof(
                y, dim_team=dim_team, fact_team_champion=fact_team_champion, team_season=team_season
            )
            for y in BACKTEST_YEARS
        ],
    )
    write_table(champion_history, CHAMPION_VALIDATION_HISTORY_TABLE)

    scorer_history = reduce(
        DataFrame.unionByName,
        [project_top_scorer_asof(y, dim_team=dim_team, skater_season=skater_season) for y in BACKTEST_YEARS],
    )
    write_table(scorer_history, SKATER_VALIDATION_HISTORY_TABLE)

    # Calibration: how often did the top-5 projected teams include the real champion?
    hit_rate = (
        load_table(spark, CHAMPION_VALIDATION_HISTORY_TABLE)
        .filter(F.col("projected_rank") <= 5)
        .agg(F.avg("actually_won_next_season").alias("top5_hit_rate"))
        .first()["top5_hit_rate"]
    )
    logger.info("Top-5 champion hit rate across backtest: %s", hit_rate)

    # Real forecast for the season the dataset never saw
    team_projection = project_champion_asof(
        FORECAST_AS_OF_YEAR,
        dim_team=dim_team,
        fact_team_champion=fact_team_champion,
        team_season=team_season,
    )
    write_table(team_projection, TEAM_PROJECTION_2021_RAW_TABLE)
    write_table(build_championship_odds(load_table(spark, TEAM_PROJECTION_2021_RAW_TABLE)), CHAMPIONSHIP_ODDS_2021_TABLE)

    write_table(
        project_top_scorer_asof(FORECAST_AS_OF_YEAR, dim_team=dim_team, skater_season=skater_season),
        BEST_PLAYER_FORECAST_2021_TABLE,
    )
    write_table(build_actual_2020_21_results(spark), ACTUAL_2020_21_RESULTS_TABLE)

    return {"top5_hit_rate": hit_rate}
