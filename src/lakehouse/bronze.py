"""Bronze: raw, append-only, exactly as published.

The discipline here is what bronze is FOR:

  * Every column lands as a STRING. Source systems do not honour their own
    schema, and casting on the way in loses the evidence of what was actually
    there. Types are applied in silver, once the values are known.
  * Never overwrite. Bronze is the replay point. If silver's logic is wrong, the
    fix is to re-run from bronze, not to go back to the source.
  * Provenance on every row: which file, when ingested, which batch. Without
    it, "where did this row come from" is unanswerable once silver rewrites it.
"""

import datetime as dt

from pyspark.sql import DataFrame, SparkSession, functions as F

from . import config


RATINGS_SCHEMA = "userId string, movieId string, rating string, timestamp string"
MOVIES_SCHEMA = "movieId string, title string, genres string"


def ingest_ratings(spark: SparkSession, batch_id: str) -> DataFrame:
    path = config.table_path(config.BRONZE_RATINGS)
    df = _read_raw(spark, config.RATINGS_CSV, batch_id, schema=RATINGS_SCHEMA)
    df.write.format("delta").mode("append").save(path)
    print(f"    bronze_ratings: appended {df.count():,} rows from ratings.csv")
    return df


def ingest_movies(spark: SparkSession, batch_id: str) -> DataFrame:
    path = config.table_path(config.BRONZE_MOVIES)
    df = _read_raw(spark, config.MOVIES_CSV, batch_id, schema=MOVIES_SCHEMA)
    df.write.format("delta").mode("append").save(path)
    print(f"    bronze_movies: appended {df.count():,} rows from movies.csv")
    return df


def ingest_corrections(spark: SparkSession, batch_id: str) -> DataFrame:
    """Late-arriving rating corrections.

    Same shape as the main feed, deliberately: a correction is not a special
    kind of record, it is a record that arrived late and supersedes an earlier
    one. Handling it through the same bronze path means the replay story stays
    true - there is one source of truth for "what we have been told".
    """
    if not config.CORRECTIONS_CSV.exists():
        print("    bronze_corrections: no corrections file, skipping")
        return None

    path = config.table_path("bronze_rating_corrections")
    df = _read_raw(
        spark,
        config.CORRECTIONS_CSV,
        batch_id,
        schema=RATINGS_SCHEMA,
        source_name="corrections",
    )
    df.write.format("delta").mode("append").save(path)
    print(f"    bronze_corrections: appended {df.count():,} rows from rating_corrections.csv")
    return df


def _read_raw(
    spark: SparkSession,
    csv_path,
    batch_id: str,
    schema: str,
    source_name: str = "primary",
) -> DataFrame:
    """Read a CSV as all-strings and stamp it with provenance.

    The schema is passed in explicitly and every field is a STRING. Letting
    Spark infer types here is the thing bronze is supposed to prevent: an
    upstream column that changes from '1.0' to '1,00' should arrive as text and
    fail loudly in silver, not be silently coerced to a number on the way in.
    """
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(str(csv_path))
    )

    return (
        df.withColumn("_source_file", F.lit(csv_path.name))
        .withColumn("_source_role", F.lit(source_name))
        .withColumn("_ingest_batch", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def read_delta(spark: SparkSession, table: str) -> DataFrame:
    """Read any Delta table in the warehouse by logical name."""
    return spark.read.format("delta").load(config.table_path(table))


def batch_id_now() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
