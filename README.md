# NHL Data Pipeline with Airflow

This is a port of [JDE10_final_project](https://github.com/kwongheng/JDE10_final_project) which was done in Microsoaft Fabric (with trial account). The port is not a one-to-one since the main objective is to use Airflow to orchestrator the pipeline instead.

It's an end-to-end data pipeline that ingests public NHL game data, cleans it through a **bronze → silver → gold** layered model, and produces team and player forecasts. It is orchestrated with **Apache Airflow**, written in **PySpark**, and stores every table as a **Delta Lake** table. Everything runs locally in Docker containers created by the **Astronomer Astro CLI**, including Spark.

The source data is the [NHL Game Data](https://www.kaggle.com/datasets/martinellis/nhl-game-data) dataset on Kaggle (`martinellis/nhl-game-data`). The pipeline uses six of its files: `game`, `player_info`, `team_info`, `game_goalie_stats`, `game_skater_stats` and `game_teams_stats`.

---

## Tech stack

| Area | Technology |
|------|------------|
| Orchestration | Apache Airflow 3 (Astro Runtime `3.4-1`) |
| Runtime / packaging | Astronomer Astro CLI, Docker Desktop (WSL 2) |
| Processing | PySpark, running in local mode inside the Airflow containers |
| Storage | Delta Lake tables on local disk (`delta-spark`) |
| Source | Kaggle, downloaded with `kagglehub` |
| Language | Python 3 |

There is no separate Spark cluster. Each Airflow task starts its own Spark session inside the container, does its work, and shuts the session down.

---

## How the code runs

The code is split into two parts so that orchestration and data logic stay separate.

- **`dags/nhl_etl_dag.py` orchestrates only.** It defines one DAG, `nhl_etl`, made of small tasks. It contains no data logic and imports nothing heavy at the top of the file, so Airflow can parse it quickly.
- **`include/nhl_etl/` does the work.** It is plain Python with no Airflow imports: Spark session creation, table helpers, ingestion, cleaning rules and forecasts.

When you trigger the DAG, every task follows the same pattern:

1. Airflow's scheduler starts the task in the container.
2. The task imports its function from `include/nhl_etl/` (imports happen inside the task, not at parse time).
3. The function opens a local Spark session with Delta Lake enabled.
4. It reads Delta tables or CSV files, transforms them with PySpark, and writes Delta tables under `include/lakehouse/`.
5. The session is closed and the task reports success or failure to Airflow.

Tables are addressed by path (for example `silver.game` is `include/lakehouse/tables/silver/game`), so tasks running in parallel, each with its own Spark session, do not need a shared metastore.

Tasks and their dependencies are **generated from `include/nhl_etl/config.py`**. That file lists the datasets and the cleaning rules for each silver table. The DAG reads it to create one bronze task and one silver task per table.

---

## Workflow

```
setup_lakehouse → land → bronze.* (6 tasks in parallel)
    silver.team_info, silver.player_info → silver.game → silver.game_teams_stats
                                                       → silver.game_skater_stats
                                                       → silver.game_goalie_stats
    → games_teams_star → gold.season_summaries → gold.forecasts
```

| Step | Task | What it does |
|------|------|--------------|
| 1 | `setup_lakehouse` | Creates the folders and two empty audit tables (`_file_manifest`, `_bronze_schema_log`). Safe to re-run. |
| 2 | `land` | Downloads the Kaggle dataset and copies the six CSV files into `include/lakehouse/landing/`. Fails if any file is missing or empty. |
| 3 | `bronze.*` | One task per dataset. Hashes the file and skips it if unchanged. Otherwise reads the CSV as-is, adds `_source_file`, `_ingestion_timestamp` and `_row_hash`, removes duplicates and inserts only new rows with a Delta `MERGE`. Schema changes are logged. |
| 4 | `silver.*` | One task per table. Reads only new bronze rows, keeps the latest row per key, fills nulls, casts dates, drops rows with unmatched foreign keys, runs data-quality checks, then upserts (SCD Type 1). A failed check stops the load. |
| 5 | `games_teams_star` | Builds the star schema: `dim_team`, `fact_team_game`, `fact_team_champion`, `fact_skater_game`, `fact_goalie_game`. |
| 6 | `gold.season_summaries` | Rolls up team, skater and goalie stats into one row per season. |
| 7 | `gold.forecasts` | Recency-weighted projections, a 2009–2018 backtest, 2020-21 championship odds and a best-player forecast. |

**Dependencies.** Each silver task waits for its own bronze task and for every silver table it has a foreign key to. For example, `silver.game` waits for `silver.team_info`, and the three stats tables wait for `silver.game`, `silver.team_info` and `silver.player_info`. The star schema waits for all its silver inputs, and gold runs last.

**Re-runs are cheap.** If the source files are unchanged, bronze skips them and silver finds no new rows. The star and gold steps still rebuild their tables.

### Output tables

| Schema | Tables |
|--------|--------|
| `bronze` | `game`, `team_info`, `player_info`, `game_goalie_stats`, `game_skater_stats`, `game_teams_stats`, plus audit tables `_file_manifest` and `_bronze_schema_log` |
| `silver` | the six cleaned tables, plus `dim_team`, `fact_team_game`, `fact_team_champion`, `fact_skater_game`, `fact_goalie_game` |
| `gold` | `team_season`, `skater_season`, `goalie_season`, `champion_validation_history`, `skater_validation_history`, `team_projection_2021_raw`, `championship_odds_2021`, `best_player_forecast_2021`, `actual_2020_21_results` |

---

## Project layout

```
airflow-pyspark/
├── Dockerfile               Astro Runtime image + Java (required by PySpark)
├── packages.txt             OS packages (empty: Java is installed in the Dockerfile)
├── requirements.txt         Python packages installed in the image
├── .env                     Kaggle credentials and optional settings (not committed)
├── dags/
│   └── nhl_etl_dag.py       The DAG: orchestration only
└── include/
    ├── nhl_etl/             ETL code (plain Python, no Airflow imports)
    │   ├── config.py        Datasets, silver rules and paths (edit to add a dataset)
    │   ├── spark_session.py SparkSession + Delta factory
    │   ├── dbutils.py       Shared load/write/validation helpers
    │   ├── bootstrap.py     Folder layout and audit tables
    │   ├── landing.py       Kaggle download
    │   ├── bronze.py        Landing → bronze
    │   ├── silver.py        Bronze → silver
    │   ├── star.py          Star schema
    │   └── gold.py          Rollups and forecasts
    └── lakehouse/           Generated data (git-ignored)
        ├── landing/         Downloaded CSVs
        └── tables/{bronze,silver,gold}/<table>/   Delta tables
```

---

## Prerequisites

Install these once on Windows 11:

1. **Docker Desktop** using the WSL 2 backend (Settings → General → "Use the WSL 2 based engine").
2. **Astro CLI** (`winget install -e --id Astronomer.Astro`). Check it with `astro version`.
3. A **Kaggle account** and API key (Not needed for this project).
4. **Memory for Docker.** Spark and Airflow together are heavy. Give Docker at least 6 GB, ideally 8 GB, in `%UserProfile%\.wslconfig`:

   ```ini
   [wsl2]
   memory=8GB
   processors=6
   ```

   Then run `wsl --shutdown` and restart Docker Desktop.
5. **Performance optimization** Based on CPU, you need to adjust `max_active_tasks` in DAG properties accordingly to avoid hanging dockers. So for CPU of 6, set ` max_active_tasks=2`. As the rule of thumb is `max_active_tasks × Spark threads ≤ CPUs given to Docker − 2` and `NHL_SPARK_MASTER=local[2]` is default. If you want to increase `max_active_tasks` then you need to reduce `NHL_SPARK_MASTER=local[1]` in .env.

---

## Project set-up (before the first run)

PySpark needs Java, the Python libraries must be in the image, and Kaggle needs credentials. Four files in the project root, `C:\dev\airflow\airflow-pyspark`, control this.

### 1. `Dockerfile`

Start from Astro Runtime (Airflow 3), install a Java runtime, and point `JAVA_HOME` at it:

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.4-1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER astro

# Pre-download the Delta Lake jars into the image so tasks never download at runtime
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; b = SparkSession.builder.master('local[1]').config('spark.ui.enabled', 'false'); s = configure_spark_with_delta_pip(b).getOrCreate(); s.stop()"
```

- The last `RUN` starts Spark once while the image is built, which downloads the Delta Lake jars into the image. Without it, every task downloads them at runtime into a shared cache, and parallel tasks corrupt each other's downloads (`Downloaded file size (0) doesn't match expected Content Length`). It needs internet during the build.
- Switch to `root` to install OS packages, then back to `astro`, the user Airflow runs as.
- Java is installed **only here**, so there is a single Java version.
- The `ln` line finds where Java was installed and links it to `/usr/lib/jvm/default-java`. Installing only `openjdk-21-jre-headless` does **not** create that folder by itself, and without it PySpark fails with `/usr/lib/jvm/default-java/bin/java: No such file or directory`. Creating the link at build time also works on both Intel/AMD and ARM machines.
- Keep the `FROM` tag at `runtime:3.4-1` unless you have a reason to upgrade.

### 2. `packages.txt`

Leave this file **empty**. It is where Astro looks for extra `apt` packages, and none are needed because Java is installed in the `Dockerfile`. Do not also list a Java package here; that would install a second Java.

### 3. `requirements.txt`

```text
kagglehub
pyspark
delta-spark
```

- `pyspark` and `delta-spark` **must be compatible versions**. Leaving both unpinned lets pip pick a matching pair.
- `kagglehub` downloads the dataset.
- Your existing file also lists `apache-airflow-providers-apache-spark`. The DAG does not use it, so it is safe to remove.
- Once a run works, pin the exact versions for repeatable builds:

  ```powershell
  astro dev bash
  pip list | grep -i -E "pyspark|delta|kagglehub"
  ```

  Copy the versions into `requirements.txt`, for example `pyspark==x.y.z`.

### 4. `.env`

Astro loads this file into every container. Add your Kaggle credentials:

```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

Optional settings (defaults shown):

```env
# Where data is written inside the container
NHL_LAKEHOUSE_DIR=/usr/local/airflow/include/lakehouse
# Spark local-mode settings; keep modest, several Spark tasks can run in parallel
NHL_SPARK_MASTER=local[2]
NHL_SPARK_DRIVER_MEMORY=2g
NHL_SPARK_SHUFFLE_PARTITIONS=8
```

`.env` is listed in `.gitignore` and `.dockerignore`, so credentials are never committed or baked into the image.

After changing any of these four files, run `astro dev restart`. Then check Java from inside the container:

```powershell
astro dev bash
java -version
echo $JAVA_HOME
ls $JAVA_HOME
```

---

## Run it

Open PowerShell in the project folder:

```powershell
cd C:\dev\airflow\airflow-pyspark
astro dev start
```

The first start builds the image and takes several minutes. When it finishes, the Airflow UI opens at **http://localhost:8080**. Astro starts five containers: Postgres, scheduler, DAG processor, API server and triggerer.

1. In the UI, open **Dags** and find **`nhl_etl`**.
2. Unpause it, then click **Trigger**.
3. Watch the **Graph** view. Click a task and open **Logs** to see its output.

The DAG has no schedule because the Kaggle dataset is static, so it only runs when you trigger it. To run it automatically, set a cron expression in `schedule=` in `dags/nhl_etl_dag.py`.

**The first image build is slow.** The last `RUN` line in the `Dockerfile` starts Spark once to download the Delta Lake jars from Maven (internet required), so they are baked into the image.

### Re-running

- **Task failed:** fix the cause, then clear the failed task in the UI. Downstream tasks resume from there. Failed tasks also retry once after 2 minutes.
- **Start from scratch:**

  ```powershell
  astro dev stop
  Remove-Item -Recurse -Force .\include\lakehouse
  astro dev start
  ```

### Querying the tables

The Delta tables are on your disk under `include/lakehouse/tables/`. To read one with Spark:

```powershell
astro dev bash
python -c "from include.nhl_etl.spark_session import build_spark; s=build_spark('q'); s.read.format('delta').load('/usr/local/airflow/include/lakehouse/tables/gold/championship_odds_2021').show()"
```

---

## Adding or changing a dataset

Edit `include/nhl_etl/config.py`; no DAG changes are needed:

1. Add a `DatasetConfig` to `DATASETS` (source file and the columns that define row identity).
2. Add a `SilverTableConfig` to `SILVER_TABLES` (columns to keep, keys, checks, foreign keys).

The DAG builds the bronze and silver tasks and their dependencies from these lists.

---

## Troubleshooting

| Symptom | Likely cause and fix |
|---------|----------------------|
| `Java gateway process exited` or `.../java: No such file or directory` | Java is missing or `JAVA_HOME` points to a folder that does not exist. Use the `Dockerfile` above (it creates the `default-java` link), then `astro dev restart`. Check with `ls /usr/lib/jvm` and `echo $JAVA_HOME` inside `astro dev bash`. |
| Land task fails with a Kaggle auth error | `KAGGLE_USERNAME` / `KAGGLE_KEY` missing or wrong in `.env`. Fix, then `astro dev restart`. |
| Task fails with `unresolved dependency`, `download failed` or `Downloaded file size (0) doesn't match expected Content Length` | The Delta jars are being downloaded at runtime and the cache (`/home/astro/.ivy2.5.2`) is corrupt or being written by parallel tasks. Make sure the `Dockerfile` has the jar pre-download line, run `astro dev restart`, then clear the failed tasks. Also check that Docker has internet and that `pyspark` and `delta-spark` versions match (`pip list` inside the container). |
| Tasks killed or the container restarts | Out of memory. Raise the WSL 2 memory limit or lower `NHL_SPARK_DRIVER_MEMORY`. |
| Spark writes are very slow | The data folder sits on a Windows bind mount. Point `NHL_LAKEHOUSE_DIR` at a Docker named volume. |
| Silver task fails with `invalid or null value(s)` or `Primary key violation` | A data-quality check stopped the load on purpose. The message names the column; inspect the matching bronze table. |
| `Ports are not available` on start | Something is using 8080 or 5432. Stop it, or [change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver). |
| Code changes do not appear | Wait a minute for the DAG processor. Changes to `Dockerfile`, `packages.txt`, `requirements.txt` or `.env` need `astro dev restart`. |

Useful commands:

```powershell
astro dev start      # build (first time) and start Airflow
astro dev stop       # pause containers, keep data
astro dev restart    # rebuild and restart after config changes
astro dev kill       # remove containers and the Airflow metadata database
astro dev logs -s    # scheduler logs
astro dev bash       # shell inside the scheduler container
```
