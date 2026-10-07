"""Quality checks and the promotion gate.

Deliberately not a framework. Every check is a named function returning a row
of (name, layer, passed, observed, expectation), and the gate is a boolean over
those rows. A dependency like Great Expectations would add hundreds of packages
to express what fits in a list comprehension here - and the interview question
is "what does your gate check and what happens when it fails", not "which
library did you use".

The failure path is the important half: a failed blocking check writes its rows
to a quarantine table and stops the pipeline BEFORE the next layer is written.
A gate that logs and continues is not a gate.
"""

import dataclasses
import datetime as dt

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from . import config


@dataclasses.dataclass
class CheckResult:
    name: str
    layer: str
    passed: bool
    observed: str
    expectation: str
    blocking: bool = True

    def as_row(self) -> dict:
        return dataclasses.asdict(self) | {"checked_at": dt.datetime.now(dt.UTC)}


def _scalar(spark: SparkSession, sql: str) -> float:
    row = spark.sql(sql).collect()
    if not row or row[0][0] is None:
        return 0.0
    return float(row[0][0])


# --- individual checks ------------------------------------------------------


def check_silver_ratings_not_empty(spark: SparkSession) -> CheckResult:
    count = _scalar(spark, f"SELECT count(*) FROM delta.`{config.table_path(config.SILVER_RATINGS)}`")
    return CheckResult(
        name="silver_ratings_not_empty",
        layer="silver",
        passed=count > 0,
        observed=f"{int(count)} rows",
        expectation="row count > 0",
    )


def check_silver_ratings_grain_unique(spark: SparkSession) -> CheckResult:
    """One rating per user-movie pair. This is the fact's grain."""
    duplicates = _scalar(
        spark,
        f"""
        SELECT count(*) FROM (
            SELECT user_id, movie_id
            FROM delta.`{config.table_path(config.SILVER_RATINGS)}`
            GROUP BY user_id, movie_id
            HAVING count(*) > 1
        )
        """,
    )
    return CheckResult(
        name="silver_ratings_grain_unique",
        layer="silver",
        passed=duplicates == 0,
        observed=f"{int(duplicates)} duplicate (user_id, movie_id) pairs",
        expectation="0 duplicates",
    )


def check_ratings_in_range(spark: SparkSession) -> CheckResult:
    """MovieLens ratings are 0.5-5.0 in 0.5 steps. Outside that is corruption."""
    path = config.table_path(config.SILVER_RATINGS)
    bad = _scalar(
        spark,
        f"""
        SELECT count(*) FROM delta.`{path}`
        WHERE rating < {config.RATING_MIN} OR rating > {config.RATING_MAX}
        """,
    )
    return CheckResult(
        name="ratings_in_range",
        layer="silver",
        passed=bad == 0,
        observed=f"{int(bad)} rows outside [{config.RATING_MIN}, {config.RATING_MAX}]",
        expectation=f"rating between {config.RATING_MIN} and {config.RATING_MAX}",
    )


def check_ratings_not_null(spark: SparkSession) -> CheckResult:
    path = config.table_path(config.SILVER_RATINGS)
    nulls = _scalar(
        spark,
        f"""
        SELECT count(*) FROM delta.`{path}`
        WHERE user_id IS NULL OR movie_id IS NULL OR rating IS NULL
        """,
    )
    return CheckResult(
        name="ratings_not_null",
        layer="silver",
        passed=nulls == 0,
        observed=f"{int(nulls)} rows with a null key or rating",
        expectation="0 nulls in user_id, movie_id, rating",
    )


def check_ratings_event_ts_not_null(spark: SparkSession) -> CheckResult:
    """Every rating must carry a usable event timestamp.

    Added after a real silent failure: bronze stores every column as a string,
    and casting a string like "1700000000" straight to timestamp yields NULL
    rather than an error. Every other check passed, the build went green, and
    event_ts was null for all 100k rows - which also broke the "latest record
    wins" dedup, because with no timestamp the ordering had nothing to order by.

    A gate only covers the rules somebody thought to write down.
    """
    nulls = _scalar(
        spark,
        f"SELECT count(*) FROM delta.`{config.table_path(config.SILVER_RATINGS)}` "
        "WHERE event_ts IS NULL",
    )
    return CheckResult(
        name="ratings_event_ts_not_null",
        layer="silver",
        passed=nulls == 0,
        observed=f"{int(nulls)} rows with a null event_ts",
        expectation="0 nulls - an unparsed epoch is a silent failure, not a clean one",
    )


def check_ratings_sane_date_range(spark: SparkSession) -> CheckResult:
    """Ratings must fall inside a plausible calendar window.

    Belt and braces after check_ratings_event_ts_not_null. A NULL timestamp is
    one failure mode; a timestamp silently read as the wrong UNIT is another,
    and it is worse because it produces plausible-looking dates rather than
    obvious nonsense.

    The bounds are deliberately wide. What this is really hunting is a unit
    error - reading Unix milliseconds as seconds lands in the year 55000, and
    reading seconds as milliseconds lands in 1970 - not a business rule about
    which decade a rating belongs to.

    The upper bound in particular has to leave room for corrections. A
    correction is by definition dated after the records it restates, so it
    legitimately postdates the end of the original dataset. Tightening this to
    the data's own era fails the pipeline on correct behaviour.
    """
    earliest = "1990-01-01"
    latest = "2030-01-01"
    outside = _scalar(
        spark,
        f"""
        SELECT count(*) FROM delta.`{config.table_path(config.SILVER_RATINGS)}`
        WHERE event_ts IS NOT NULL
          AND (CAST(event_ts AS DATE) < DATE '{earliest}'
               OR CAST(event_ts AS DATE) >= DATE '{latest}')
        """,
    )
    return CheckResult(
        name="ratings_in_sane_date_range",
        layer="silver",
        passed=outside == 0,
        observed=f"{int(outside)} rows outside [{earliest}, {latest})",
        expectation="catches epoch-unit errors (1970 or year 55000); "
        "wide enough to allow corrections dated after the dataset ends",
    )


def check_no_orphans(spark: SparkSession) -> CheckResult:
    """Every rated movie must exist in the movie catalogue.

    Runs on SILVER, not gold. Gold's fact inner-joins to dim_movie, so by the
    time the fact exists the orphans have already been silently dropped and this
    check could never fail. Checking the conformed layer is what makes it a real
    gate rather than a decoration.
    """
    orphan = _scalar(
        spark,
        f"""
        SELECT count(*)
        FROM delta.`{config.table_path(config.SILVER_RATINGS)}` r
        LEFT ANTI JOIN delta.`{config.table_path(config.SILVER_MOVIES)}` m
          ON r.movie_id = m.movie_id
        """,
    )
    return CheckResult(
        name="fk_movie_exists",
        layer="silver",
        passed=orphan == 0,
        observed=f"{int(orphan)} ratings reference an unknown movie_id",
        expectation="0 orphaned ratings - they should be quarantined, not present",
    )


def check_quarantine_captured_orphans(spark: SparkSession) -> CheckResult:
    """The deliberately-bad correction rows must be parked, not lost.

    Non-obvious but worth asserting: the generator injects orphan ratings and an
    out-of-range rating on every run, so if quarantine is empty the rows were
    dropped somewhere instead of being retained. An exclusion you cannot count
    is not an exclusion you can defend.
    """
    parked = _scalar(
        spark,
        f"SELECT count(*) FROM delta.`{config.table_path(config.QUARANTINE_RATINGS)}`",
    )
    return CheckResult(
        name="quarantine_retained_bad_rows",
        layer="silver",
        passed=parked > 0,
        observed=f"{int(parked)} rows in quarantine_ratings",
        expectation="> 0 - injected bad rows are parked, not silently dropped",
    )


def check_gold_fact_grain_unique(spark: SparkSession) -> CheckResult:
    path = config.table_path(config.GOLD_FACT_RATINGS)
    duplicates = _scalar(
        spark,
        f"""
        SELECT count(*) FROM (
            SELECT user_key, movie_key
            FROM delta.`{path}`
            GROUP BY user_key, movie_key
            HAVING count(*) > 1
        )
        """,
    )
    return CheckResult(
        name="gold_fact_grain_unique",
        layer="gold",
        passed=duplicates == 0,
        observed=f"{int(duplicates)} duplicate (user_key, movie_key) pairs",
        expectation="0 duplicates - MERGE must upsert, not append",
    )


def check_corrections_applied(spark: SparkSession) -> CheckResult:
    """Proves the MERGE actually corrected rows rather than appending them.

    Non-blocking in spirit but blocking in practice: if the corrections did not
    land, the whole point of the MERGE demonstration has failed.
    """
    path = config.table_path(config.GOLD_FACT_RATINGS)
    superseded = _scalar(
        spark,
        f"""
        SELECT count(*) FROM delta.`{path}`
        WHERE rating_source = 'correction'
        """,
    )
    return CheckResult(
        name="corrections_applied",
        layer="gold",
        passed=superseded > 0,
        observed=f"{int(superseded)} rows sourced from the correction batch",
        expectation="> 0 - the MERGE upsert landed",
    )


CHECKS_BY_LAYER = {
    "silver": [
        check_silver_ratings_not_empty,
        check_silver_ratings_grain_unique,
        check_ratings_in_range,
        check_ratings_not_null,
        check_ratings_event_ts_not_null,
        check_ratings_sane_date_range,
        check_no_orphans,
        check_quarantine_captured_orphans,
    ],
    "gold": [
        check_gold_fact_grain_unique,
        check_corrections_applied,
    ],
}


# --- gate -------------------------------------------------------------------


class QualityGateFailed(RuntimeError):
    """Raised when a blocking check fails. Stops the pipeline before promotion."""

    def __init__(self, layer: str, failures: list[CheckResult]):
        self.layer = layer
        self.failures = failures
        detail = "; ".join(f"{f.name}: {f.observed}" for f in failures)
        super().__init__(f"quality gate failed for {layer}: {detail}")


def run_gate(spark: SparkSession, layer: str) -> list[CheckResult]:
    """Run every check for a layer, persist the results, and fail if any did.

    Results are always written, pass or fail - a gate whose history only exists
    when it breaks is not evidence of anything.
    """
    results: list[CheckResult] = []
    for check in CHECKS_BY_LAYER.get(layer, []):
        try:
            results.append(check(spark))
        except Exception as error:
            # A check that cannot run has not passed. Treating an exception as
            # "fine" is how quality gates quietly stop working.
            results.append(
                CheckResult(
                    name=check.__name__,
                    layer=layer,
                    passed=False,
                    observed=f"check raised {type(error).__name__}: {error}",
                    expectation="check runs to completion",
                )
            )

    persist_results(spark, results)

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"    [{status}] {result.name}: {result.observed}")

    failures = [r for r in results if not r.passed and r.blocking]
    if failures:
        raise QualityGateFailed(layer, failures)

    return results


def persist_results(spark: SparkSession, results: list[CheckResult]) -> None:
    if not results:
        return
    df = spark.createDataFrame([r.as_row() for r in results])
    path = config.table_path(config.DQ_RESULTS)
    df.write.format("delta").mode("append").save(path)


def quarantine(spark: SparkSession, layer: str, table: str, reason: str) -> None:
    """Park rows that failed a rule, with the reason attached.

    Quarantine rather than delete: an analyst whose revenue report is short
    needs to see what was excluded, not find rows silently missing. Excluding
    rows is an operational decision, and this is where it becomes visible.
    """
    df = spark.read.format("delta").load(config.table_path(table))
    bad = df.withColumn("quarantine_reason", _lit(reason)).withColumn(
        "quarantined_at", _lit_now()
    )
    bad.write.format("delta").mode("append").save(config.table_path(config.QUARANTINE_RATINGS))
    print(f"    quarantined {bad.count()} rows from {table}: {reason}")


def _lit(value: str):
    from pyspark.sql import functions as F

    return F.lit(value)


def _lit_now():
    from pyspark.sql import functions as F

    return F.current_timestamp()


def delta_version(spark: SparkSession, table: str) -> int:
    """Current version of a Delta table, for the time-travel demonstrations."""
    return DeltaTable.forPath(spark, config.table_path(table)).history(1).collect()[0]["version"]
