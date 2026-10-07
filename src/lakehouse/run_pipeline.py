"""Pipeline entrypoint. Every stage is a callable the Airflow DAG can invoke.

The DAG calls these with --stage; running this file with no argument runs the
whole thing in order, which is what a reviewer wants and what CI runs.
"""

import argparse
import sys
import time

from . import bronze, config, gold, quality, silver
from .quality import QualityGateFailed
from .session import get_spark, stop_spark


def stage_fetch() -> None:
    """Pull source data and generate the correction batch."""
    import pathlib

    sys.path.insert(0, str(config.PROJECT_ROOT / "scripts"))
    import fetch_data  # type: ignore
    import make_corrections  # type: ignore

    fetch_data.main()
    make_corrections.main()


def stage_bronze(spark) -> None:
    """Land raw files append-only, with provenance on every row."""
    batch = bronze.batch_id_now()
    print(f"  batch_id={batch}")
    bronze.ingest_ratings(spark, batch)
    bronze.ingest_movies(spark, batch)
    bronze.ingest_corrections(spark, batch)


def stage_silver(spark) -> None:
    """Conform, deduplicate, quarantine - then gate before promotion."""
    silver.run(spark)
    quality.run_gate(spark, "silver")


def stage_gold(spark) -> None:
    """Build dimensions and MERGE the fact, then gate."""
    gold.run(spark)
    quality.run_gate(spark, "gold")


STAGES = {
    "fetch": stage_fetch,
    "bronze": stage_bronze,
    "silver": stage_silver,
    "gold": stage_gold,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=["all", *STAGES],
        default="all",
        help="run a single stage, or the whole pipeline in order",
    )
    args = parser.parse_args()

    started = time.time()

    if args.stage == "fetch":
        stage_fetch()
        return 0

    spark = get_spark("lakehouse-etl")

    try:
        if args.stage == "all":
            # fetch runs first on a full run so a fresh clone needs one command,
            # but it needs no Spark session, so it stays out of the stage loop.
            stage_fetch()
            stages = ["bronze", "silver", "gold"]
        else:
            stages = [args.stage]

        for name in stages:
            print(f"\n=== {name} ===")
            stage_started = time.time()
            STAGES[name](spark)
            print(f"    ({time.time() - stage_started:.1f}s)")

    except QualityGateFailed as failure:
        print(f"\n*** QUALITY GATE FAILED ***\n{failure}", file=sys.stderr)
        print(
            "\nThe next layer was NOT written. dq_results holds the full check "
            "history; quarantine_ratings holds the rows that failed.",
            file=sys.stderr,
        )
        return 2
    finally:
        stop_spark(spark)

    print(f"\n=== done in {time.time() - started:.1f}s ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
