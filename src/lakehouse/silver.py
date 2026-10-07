"""Silver: conformed, deduplicated, validated.

This is where the pipeline makes decisions that are expensive to reverse, so
each one is deliberate:

  * TYPES ARE APPLIED HERE, not in bronze. Bronze kept everything as text so
    that a malformed value shows up as a value rather than as a null nobody can
    explain.
  * CORRECTIONS AND ORIGINALS ARE UNIONED, then deduplicated by taking the row
    with the highest timestamp per (user_id, movie_id). A correction is not a
    special record - it is a later truth about the same key. Unioning first and
    deduplicating second means the precedence rule lives in exactly one place.
  * INVALID ROWS ARE QUARANTINED, not dropped, and never silently.
"""

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from . import bronze, config


def build_ratings(spark: SparkSession, write: bool = True) -> DataFrame:
    """Union the primary feed with corrections, then keep the latest per key."""
    ratings = (
        bronze.read_delta(spark,  config.BRONZE_RATINGS)
        .withColumnRenamed("userId", "user_id_raw")
        .withColumnRenamed("movieId", "movie_id_raw")
        .withColumnRenamed("timestamp", "event_ts_raw")
    )

    corrections = None
    try:
        corrections = (
            bronze.read_delta(spark,  "bronze_rating_corrections")
            .withColumnRenamed("userId", "user_id_raw")
            .withColumnRenamed("movieId", "movie_id_raw")
            .withColumnRenamed("timestamp", "event_ts_raw")
        )
    except Exception:
        corrections = None

    unioned = (
        ratings.withColumn("rating_source", F.lit("primary"))
        if corrections is None
        else ratings.withColumn("rating_source", F.lit("primary")).unionByName(
            corrections.withColumn("rating_source", F.lit("correction"))
        )
    )

    typed = (
        unioned.withColumn("user_id", F.col("user_id_raw").cast("long"))
        .withColumn("movie_id", F.col("movie_id_raw").cast("long"))
        .withColumn("rating", F.col("rating").cast("double"))
        # MovieLens timestamps are Unix SECONDS, and bronze holds them as
        # STRINGS. So this needs two casts, and the order matters:
        #
        #   string -> long   parses "1700000000" as the number 1700000000
        #   long -> timestamp  reads that as SECONDS since epoch (2000-07-30
        #                     for the first MovieLens rating)
        #
        # Casting the string straight to timestamp skips the first step and
        # tries to parse "1700000000" as a timestamp literal, which is not one -
        # so every event_ts silently becomes NULL. That did not fail the build:
        # the gate checked user_id, movie_id and rating, and never the timestamp.
        .withColumn(
            "event_ts", F.col("event_ts_raw").cast("long").cast("timestamp")
        )
        .drop("user_id_raw", "movie_id_raw", "event_ts_raw")
    )

    # Keep the newest record per (user_id, movie_id). row_number over a window
    # partitioned by the key, ordered by event_ts descending - so a correction
    # wins over the original it supersedes, and two corrections resolve to the
    # latest one.
    latest = Window.partitionBy("user_id", "movie_id").orderBy(
        F.col("event_ts").desc(), F.col("_ingested_at").desc()
    )

    deduped = (
        typed.withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Everything that cannot be trusted goes to quarantine with its reason.
    # Flag first, then split on the flag - one expression, evaluated once, so
    # the rule that quarantines a row is visibly the same rule that keeps it.
    flagged = deduped.withColumn(
        "_is_invalid",
        F.col("user_id").isNull()
        | F.col("movie_id").isNull()
        | F.col("rating").isNull()
        | (F.col("rating") < config.RATING_MIN)
        | (F.col("rating") > config.RATING_MAX),
    )
    invalid = flagged.filter(F.col("_is_invalid"))
    clean = flagged.filter(~F.col("_is_invalid")).drop("_is_invalid")

    if invalid.count() > 0:
        _quarantine(
            invalid,
            f"rating outside [{config.RATING_MIN}, {config.RATING_MAX}], "
            "or null user_id/movie_id/rating",
        )

    # Referential integrity is enforced HERE, in the conformed layer, not in
    # gold. Gold's fact inner-joins to dim_movie, which silently drops orphans
    # - so an orphan check written against gold can never fail, because the join
    # has already removed the evidence. Catching it here means the gate has
    # something real to stop.
    movies = bronze.read_delta(spark, config.SILVER_MOVIES).select("movie_id")
    orphaned = clean.join(F.broadcast(movies), on="movie_id", how="left_anti")
    clean = clean.join(F.broadcast(movies), on="movie_id", how="left_semi")

    if orphaned.count() > 0:
        _quarantine(orphaned, "movie_id not present in the movie catalogue")

    enriched = clean.withColumn(
        "rating_tier",
        F.when(F.col("rating") >= 4.5, F.lit("high"))
        .when(F.col("rating") >= 3.0, F.lit("medium"))
        .otherwise(F.lit("low")),
    ).withColumn("rated_month", F.date_format(F.col("event_ts"), "yyyy-MM"))

    if write:
        enriched.write.format("delta").mode("overwrite").save(
            config.table_path(config.SILVER_RATINGS)
        )
        print(f"    silver_ratings: wrote {enriched.count():,} rows")

    return enriched


def build_movies(spark: SparkSession, write: bool = True) -> DataFrame:
    """Conform movies, and split the pipe-delimited genre string.

    A multi-valued attribute stored as one delimited string is the single
    most common modelling mistake in a first star schema: it makes "which genre
    is most popular" require string splitting in every query. Splitting it once
    into an array here is cheaper than teaching every consumer to do it.
    """
    movies = (
        bronze.read_delta(spark, config.BRONZE_MOVIES)
        .withColumn("movie_id", F.col("movieId").cast("long"))
        .withColumn("title", F.trim(F.col("title")))
        # Rename the raw delimited string BEFORE building the array column of
        # the same name. Dropping "genres" after creating an array called
        # "genres" drops the array too.
        .withColumn("genres_raw", F.col("genres"))
        .withColumn(
            "genres",
            F.array_remove(
                F.split(
                    F.when(F.col("genres_raw") == "(no genres listed)", F.lit("Unknown"))
                    .otherwise(F.col("genres_raw")),
                    "\\|",
                ),
                "",
            ),
        )
        .withColumn("primary_genre", F.element_at("genres", 1))
        .withColumn(
            "release_year",
            F.regexp_extract(F.col("title"), r"\((\d{4})\)\s*$", 1).cast("int"),
        )
        .drop("movieId", "genres_raw")
        .dropDuplicates(["movie_id"])
    )

    if write:
        movies.write.format("delta").mode("overwrite").save(
            config.table_path(config.SILVER_MOVIES)
        )
        print(f"    silver_movies: wrote {movies.count():,} rows")

    return movies


def run(spark: SparkSession) -> None:
    build_movies(spark)
    build_ratings(spark)


def _quarantine(df: DataFrame, reason: str) -> None:
    """Park rows that failed a rule, with the reason attached.

    Quarantine rather than delete: an analyst whose report is short needs to be
    able to see what was excluded and why, not find rows silently missing.
    Excluding rows is an operational decision, and this is where it becomes
    visible and countable.
    """
    parked = (
        df.withColumn("quarantine_reason", F.lit(reason))
        .withColumn("quarantined_at", F.current_timestamp())
    )
    parked.write.format("delta").mode("append").save(
        config.table_path(config.QUARANTINE_RATINGS)
    )
    print(f"    quarantined {df.count():,} rows: {reason}")
