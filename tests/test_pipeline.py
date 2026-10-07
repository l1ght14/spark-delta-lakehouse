"""Tests.

Two tiers, deliberately:

  * FAST tests need no Spark. They cover the parts that can silently drift -
    the correction generator's determinism, the config paths - because those are
    the things that make a rebuild produce different numbers.

  * SLOW tests need a Spark session (~25s to start) and assert the invariants
    the pipeline claims: grain, quarantine, and that re-running MERGEs rather
    than appends.

Run the fast tier alone with:  pytest tests -m "not slow"
"""

import pathlib
import sys

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from src.lakehouse import config  # noqa: E402


# ---------------------------------------------------------------- fast tier


def test_table_paths_are_namespaced_under_the_warehouse_root():
    """Every table must resolve inside the warehouse, not into the project dir.

    The warehouse deliberately lives on WSL-native ext4 rather than /mnt/d; a
    path that escaped the root would put Delta commits on a filesystem that
    cannot do atomic renames.
    """
    for name in (
        config.BRONZE_RATINGS,
        config.SILVER_RATINGS,
        config.GOLD_FACT_RATINGS,
        config.QUARANTINE_RATINGS,
    ):
        path = pathlib.Path(config.table_path(name))
        assert path.is_relative_to(config.LAKEHOUSE_ROOT)
        assert path.name == name


def test_corrections_are_deterministic():
    """Same seed must produce a byte-identical file.

    A correction batch that changed between runs would make the MERGE
    demonstration unreproducible and any row-count comparison meaningless.
    """
    import make_corrections

    first = config.CORRECTIONS_CSV.read_bytes()
    assert make_corrections.main() == 0
    second = config.CORRECTIONS_CSV.read_bytes()
    assert first == second, "correction generator is not deterministic"


def test_corrections_inject_exactly_what_the_pipeline_expects():
    """The generator and the pipeline must agree on the bad-row counts.

    These two numbers are load-bearing: the quality gate asserts quarantine is
    non-empty precisely because these rows exist, and the README quotes them.
    """
    import csv

    import make_corrections

    make_corrections.main()
    with open(config.CORRECTIONS_CSV, encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == (
        make_corrections.N_CORRECTIONS
        + make_corrections.N_UNKNOWN_MOVIE
        + make_corrections.OUT_OF_RANGE
    )

    out_of_range = [r for r in rows if float(r["rating"]) > config.RATING_MAX]
    assert len(out_of_range) == make_corrections.OUT_OF_RANGE

    unknown_movie = [r for r in rows if r["movieId"] == "99999999"]
    assert len(unknown_movie) == make_corrections.N_UNKNOWN_MOVIE

    # Corrections must sort AFTER the base data or "latest wins" picks the
    # original instead of the correction and the whole demo silently no-ops.
    base_max = 1_576_822_419  # last MovieLens timestamp, Dec 2018
    assert all(int(r["timestamp"]) > base_max for r in rows)


# ---------------------------------------------------------------- slow tier


@pytest.mark.slow
def test_pipeline_invariants():
    """Run the pipeline and assert the claims the README makes."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from src.lakehouse import gold, silver
    from src.lakehouse.session import get_spark, stop_spark

    spark = get_spark("pytest")
    try:
        silver.run(spark)
        gold.run(spark)
    finally:
        stop_spark(spark)

    spark = get_spark("pytest-assert")
    try:
        fact_path = config.table_path(config.GOLD_FACT_RATINGS)

        # 1. The fact grain holds: one row per (user_key, movie_key).
        duplicates = spark.sql(
            f"""
            SELECT count(*) FROM (
                SELECT user_key, movie_key FROM delta.`{fact_path}`
                GROUP BY user_key, movie_key HAVING count(*) > 1
            )
            """
        ).collect()[0][0]
        assert duplicates == 0, f"{duplicates} duplicate grain keys in the fact"

        # 2. Re-running MERGEs rather than appends.
        before_count = spark.read.format("delta").load(fact_path).count()
        before_version = DeltaTable.forPath(spark, fact_path).history(1).collect()[0]["version"]

        spark = get_spark("pytest-rerun")
        try:
            gold.run(spark)
        finally:
            stop_spark(spark)

        spark = get_spark("pytest-assert2")
        after_count = spark.read.format("delta").load(fact_path).count()
        after_version = DeltaTable.forPath(spark, fact_path).history(1).collect()[0]["version"]

        assert after_count == before_count, (
            f"row count changed on re-run: {before_count} -> {after_count}. "
            "MERGE appended instead of upserting."
        )
        assert after_version > before_version, "re-run did not create a new Delta version"

        # 3. Time travel recovers the pre-MERGE state.
        assert DeltaTable.isDeltaTable(spark, fact_path)

        # 4. The injected bad rows are parked, not dropped.
        quarantined = spark.read.format("delta").load(
            config.table_path(config.QUARANTINE_RATINGS)
        ).count()
        assert quarantined >= 4, (
            f"expected at least the 4 injected bad rows in quarantine, found {quarantined}"
        )

        # 5. event_ts actually parsed - the check added after it silently didn't.
        null_ts = spark.sql(
            f"SELECT count(*) FROM delta.`{config.table_path(config.SILVER_RATINGS)}` "
            "WHERE event_ts IS NULL"
        ).collect()[0][0]
        assert null_ts == 0, f"{null_ts} rows have a null event_ts"

        # 6. Sanity: the era of the data is plausible.
        earliest, latest = spark.sql(
            f"SELECT min(event_ts), max(event_ts) FROM delta.`{config.table_path(config.SILVER_RATINGS)}`"
        ).collect()[0]
        assert earliest.year == 1996, f"unexpected earliest rating: {earliest}"
        assert 2018 <= latest.year <= 2024, f"unexpected latest rating: {latest}"
    finally:
        stop_spark(spark)
