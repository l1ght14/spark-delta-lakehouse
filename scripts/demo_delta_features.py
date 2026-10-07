"""Demonstrate the four Delta features this project depends on.

Run after the pipeline. Each section proves one capability and prints the
evidence, because "Delta supports time travel" is a claim a reviewer cannot
check from a README - but a script that prints version 0 alongside version 1 is
something they can run in ten seconds.

  1. ACID commits        - every operation leaves a version; nothing is partial
  2. MERGE upsert        - a late correction lands in place, not as a duplicate
  3. TIME TRAVEL         - the pre-correction value is still readable
  4. DESCRIBE HISTORY    - the audit trail of who changed what, and when
"""

import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.lakehouse import config
from src.lakehouse.session import get_spark

FACT = config.GOLD_FACT_RATINGS


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def main() -> int:
    spark = get_spark("delta-demo")
    path = config.table_path(FACT)

    if not DeltaTable.isDeltaTable(spark, path):
        print(f"  {path} is not a Delta table. Run the pipeline first.")
        return 1

    table = DeltaTable.forPath(spark, path)
    before_version = table.history(1).collect()[0]["version"]
    before_count = table.toDF().count()
    print(f"\n  current version : {before_version}")
    print(f"  current rows    : {before_count:,}")

    # ---------------------------------------------------------------- 1 & 2
    section("1+2. MERGE a new correction, then 3. read the OLD value back")

    # Pick a row that is currently rated below the top band, so the change is
    # visible as a real difference rather than a rounding artefact.
    target = (
        table.toDF()
        .filter(F.col("rating") < 4.0)
        .orderBy(F.col("rating"), "movie_key")
        .limit(1)
        .collect()[0]
    )
    old_rating = target["rating"]
    new_rating = 5.0
    print(f"  target pair     : user_key={target['user_key'][:12]}... movie_key={target['movie_key'][:12]}...")
    print(f"  rating now      : {old_rating}")
    print(f"  corrected to    : {new_rating}")

    staged = spark.createDataFrame(
        [
            (
                target["user_key"],
                target["movie_key"],
                new_rating,
                "high",
                target["event_ts"],
                target["rated_month"],
                "correction",
                "correction",
                "delta-demo",
            )
        ],
        [
            "user_key",
            "movie_key",
            "rating",
            "rating_tier",
            "event_ts",
            "rated_month",
            "rating_source",
            "_source_role",
            "_ingest_batch",
        ],
    )

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(staged.alias("s"), "t.user_key = s.user_key AND t.movie_key = s.movie_key")
        .whenMatchedUpdateAll()
        .execute()
    )

    after = DeltaTable.forPath(spark, path)
    after_version = after.history(1).collect()[0]["version"]
    after_count = after.toDF().count()

    print(f"\n  version after   : {after_version}")
    print(f"  rows after      : {after_count:,}  (was {before_count:,})")
    if after_count == before_count:
        print("  -> MERGE updated in place. No duplicate row was appended.")
    else:
        print(f"  -> UNEXPECTED: row count changed by {after_count - before_count}")

    current = (
        spark.read.format("delta")
        .load(path)
        .filter(
            (F.col("user_key") == target["user_key"])
            & (F.col("movie_key") == target["movie_key"])
        )
        .collect()[0]["rating"]
    )
    historical = (
        spark.read.format("delta")
        .option("versionAsOf", before_version)
        .load(path)
        .filter(
            (F.col("user_key") == target["user_key"])
            & (F.col("movie_key") == target["movie_key"])
        )
        .collect()[0]["rating"]
    )

    print(f"\n  TIME TRAVEL")
    print(f"    version {after_version} (now)      : rating = {current}")
    print(f"    version {before_version} (before) : rating = {historical}")
    if current == new_rating and historical == old_rating:
        print("  -> The pre-correction value is still readable. The update was")
        print("     committed as a new version, not an in-place overwrite.")
    else:
        print("  -> UNEXPECTED: time travel did not return the old value")
        return 1

    # -------------------------------------------------------------------- 4
    section("4. DESCRIBE HISTORY - the audit trail")
    # Row has no .get(); asDict() is the supported accessor.
    rows = [r.asDict() for r in DeltaTable.forPath(spark, path).history(5).collect()]
    print(f"  {'version':>8}  {'timestamp':<21} {'operation':<10} details")
    for row in rows:
        ts = str(row.get("timestamp"))[:19]
        print(f"  {row['version']:>8}  {ts:<21} {row['operation']:<10}")
        metrics = row.get("operationMetrics") or {}
        for key in ("numAddedFiles", "numRemovedFiles", "numUpdatedRows", "numCopiedRows"):
            if key in metrics:
                print(f"{'':>10}  {key} = {metrics[key]}")

    print("\n  Every row above is a committed version. A reader that started")
    print("  before the MERGE and finished after it still sees a consistent")
    print("  snapshot - that is the ACID guarantee, on plain files.")

    section("gold_fact_rating schema")
    schema = spark.read.format("delta").load(path).schema
    for field in schema.fields:
        print(f"  {field.name:<20} {field.dataType.simpleString()}")

    spark.stop()
    print("\n  DELTA DEMO COMPLETE")
    return 0


if __name__ == "__main__":
    sys.exit(main())