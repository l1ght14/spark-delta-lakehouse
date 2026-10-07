"""Gold: the dimensional model consumers actually query.

Grain of fct_rating: ONE ROW PER (user_id, movie_id) PAIR - one user's rating of
one movie. Not one row per rating event, because a user who re-rates is stating
a new fact about a relationship that already exists, not creating a second one.
That is the whole reason this table is MERGEd rather than overwritten.

Why MERGE and not overwrite:
  Overwriting gold on every run is the simple answer and it is wrong twice over.
  It discards history - a fact table that has been rewritten five times has no
  sixth version worth anything. And it makes a partial failure expensive: a
  re-run that dies halfway leaves consumers reading a table that is neither the
  old one nor the new one.

  MERGE upserts on the grain key, so corrections land in place, the table stays
  at exactly one row per pair, and every intermediate state is a committed
  Delta version that time travel can still reach.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from . import bronze, config


def build_dim_movie(spark: SparkSession) -> DataFrame:
    """Movie dimension, Type 1.

    A surrogate key hashed from the natural key, because the gold layer must be
    joinable to its own tables even if the source's ids are ever renumbered.
    The natural key is kept alongside it so the two can be reconciled - a
    warehouse you cannot reconcile to its source is a warehouse you cannot
    audit.
    """
    movies = bronze.read_delta(spark, config.SILVER_MOVIES)

    dim = (
        movies.select(
            F.md5(F.concat(F.lit("movie|"), F.col("movie_id").cast("string"))).alias(
                "movie_key"
            ),
            F.col("movie_id").alias("movie_natural_key"),
            "title",
            "primary_genre",
            "genres",
            "release_year",
        )
        .withColumn("decade", (F.col("release_year") / 10).cast("int") * 10)
        .orderBy("movie_natural_key")
    )

    _overwrite(dim, config.GOLD_DIM_MOVIE, "dim_movie")
    print(f"    dim_movie: {dim.count():,} rows")
    return dim


def build_dim_user(spark: SparkSession) -> DataFrame:
    """User dimension, Type 1.

    Users have no attributes in MovieLens beyond their id, which is exactly the
    case where a dimension is still worth building: it gives the fact a
    surrogate key and gives the serving layer a place for lifetime stats that
    would otherwise be recomputed per query.
    """
    ratings = bronze.read_delta(spark, config.SILVER_RATINGS)

    dim = ratings.groupBy("user_id").agg(
        F.count("*").alias("rating_count"),
        F.min("event_ts").alias("first_rating_at"),
        F.max("event_ts").alias("last_rating_at"),
        F.round(F.avg("rating"), 3).alias("avg_rating_given"),
    ).select(
        F.md5(F.concat(F.lit("user|"), F.col("user_id").cast("string"))).alias("user_key"),
        F.col("user_id").alias("user_natural_key"),
        "rating_count",
        "first_rating_at",
        "last_rating_at",
        "avg_rating_given",
    )

    _overwrite(dim, config.GOLD_DIM_USER, "dim_user")
    print(f"    dim_user: {dim.count():,} rows")
    return dim


def build_fact_ratings(spark: SparkSession) -> DataFrame:
    """The fact table, MERGEd onto its grain key.

    First run creates the table. Every run after that upserts, so a correction
    batch lands as an update rather than a duplicate row. The quality gate's
    gold_fact_grain_unique check is what proves the upsert did not append.
    """
    ratings = bronze.read_delta(spark, config.SILVER_RATINGS)
    dim_movie = bronze.read_delta(spark, config.GOLD_DIM_MOVIE)
    dim_user = bronze.read_delta(spark, config.GOLD_DIM_USER)

    staged = (
        ratings.join(
            F.broadcast(dim_movie.select("movie_key", "movie_natural_key")),
            ratings.movie_id == F.col("movie_natural_key"),
            "inner",
        )
        .join(
            F.broadcast(dim_user.select("user_key", "user_natural_key")),
            ratings.user_id == F.col("user_natural_key"),
            "inner",
        )
        .select(
            "user_key",
            "movie_key",
            "rating",
            "rating_tier",
            "event_ts",
            "rated_month",
            "rating_source",
            "_source_role",
            "_ingest_batch",
        )
    )

    path = config.table_path(config.GOLD_FACT_RATINGS)

    if not DeltaTable.isDeltaTable(spark, path):
        staged.write.format("delta").mode("overwrite").save(path)
        print(f"    fct_rating: created with {staged.count():,} rows")
    else:
        target = DeltaTable.forPath(spark, path)
        before = target.history(1).collect()[0]["version"]
        (
            target.alias("t")
            .merge(staged.alias("s"), "t.user_key = s.user_key AND t.movie_key = s.movie_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        after = DeltaTable.forPath(spark, path).history(1).collect()[0]["version"]
        print(
            f"    fct_rating: MERGEd {staged.count():,} rows "
            f"(version {before} -> {after})"
        )

    return bronze.read_delta(spark, config.GOLD_FACT_RATINGS)


def build_monthly_agg(spark: SparkSession) -> DataFrame:
    """Serving table: one row per movie per month.

    An analyst asking "how did Toy Story do in 1997" should not aggregate the
    fact on every dashboard load. This is the reason the gold layer exists.
    """
    fact = bronze.read_delta(spark, config.GOLD_FACT_RATINGS)
    dim_movie = bronze.read_delta(spark, config.GOLD_DIM_MOVIE)

    agg = (
        fact.join(dim_movie, "movie_key")
        .groupBy("movie_key", "rated_month", "title", "primary_genre", "release_year")
        .agg(
            F.count("*").alias("rating_count"),
            F.round(F.avg("rating"), 3).alias("avg_rating"),
            F.countDistinct("user_key").alias("distinct_raters"),
        )
    )

    _overwrite(agg, config.GOLD_AGG_MONTHLY, "agg_monthly_movie_ratings")
    print(f"    agg_monthly_movie_ratings: {agg.count():,} rows")
    return agg


def _overwrite(df: DataFrame, table: str, label: str) -> None:
    """Dimensions are rebuilt wholesale; only the fact is MERGEd.

    A dimension is small and fully derived, so replacing it is cheap and removes
    any chance of stale members. The fact is different - it is the thing with
    history, so it is the thing that gets upserted instead.
    """
    df.write.format("delta").mode("overwrite").save(config.table_path(table))


def run(spark: SparkSession) -> None:
    build_dim_movie(spark)
    build_dim_user(spark)
    build_fact_ratings(spark)
    build_monthly_agg(spark)
