"""Airflow DAG for the medallion lakehouse.

Design decisions worth defending:

  * The quality gate is a SEPARATE TASK, not buried inside the layer that
    produces the data. As a task it has its own run history, its own retry
    policy, and it can fail a run without the layer that produced the bad data
    being blamed for it. "silver built successfully but the gate rejected it" is
    a different fact from "silver failed".

  * THE GOLD TASKS HAVE retries=0. A gate exists to stop bad data reaching
    consumers; retrying downstream work after a gate has rejected its input just
    re-runs the same work against the same bad input. Retrying is for
    infrastructure failure (a dropped connection, an OOM kill), not for a check
    that said no.

  * catchup=False. This pipeline is triggered manually or on a schedule; there
    is no history to backfill, and backfilling 100k-row loads nobody asked for is
    a bad default.

  * Each task builds its own SparkSession in its own process. That costs about
    25 seconds of JVM startup per task, and it is the right trade here: a shared
    session across tasks is only possible in-process, and Airflow's whole model
    is that tasks are isolated processes. The isolation is the feature.
"""

import os
import pathlib
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.environ.get("LAKEHOUSE_PROJECT_ROOT") or str(
    pathlib.Path(__file__).resolve().parents[1]
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.lakehouse import config  # noqa: E402
from src.lakehouse.quality import QualityGateFailed, run_gate  # noqa: E402
from src.lakehouse.session import get_spark, stop_spark  # noqa: E402

# --- task callables ---------------------------------------------------------


def task_fetch(**_) -> None:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))
    import fetch_data
    import make_corrections

    fetch_data.main()
    make_corrections.main()


def _with_spark(fn, **kwargs):
    """Run fn with a live session, always tearing it down.

    The try/finally is the point: without it a task that raises leaves a JVM and
    its executor threads alive, and the next task on the machine runs out of
    memory for no reason anyone can find.
    """

    def runner(**task_kwargs):
        spark = get_spark("airflow")
        try:
            fn(spark, **kwargs)
        finally:
            stop_spark(spark)

    return runner


def _bronze(spark):
    from src.lakehouse import bronze

    batch = bronze.batch_id_now()
    print(f"  batch_id={batch}")
    bronze.ingest_ratings(spark, batch)
    bronze.ingest_movies(spark, batch)
    bronze.ingest_corrections(spark, batch)


def _silver(spark):
    from src.lakehouse import silver

    silver.run(spark)


def _gold(spark):
    from src.lakehouse import gold

    gold.run(spark)


def _gate(layer):
    def run(spark):
        run_gate(spark, layer)

    return run


# --- DAG --------------------------------------------------------------------

with DAG(
    dag_id="lakehouse_medallion",
    description="Bronze -> Silver -> Gold on Delta Lake, with a quality gate between layers",
    schedule=None,  # manual or externally triggered; see catchup note below
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Two tries with a pause, because the realistic failure here is a transient
    # one - a dropped mount, a contended JVM. A deterministic failure will fail
    # twice too, which is acceptable; an unbounded retry would not be.
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "owner": "data-platform",
    },
    tags=["delta", "medallion", "pyspark"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_raw_data",
        python_callable=task_fetch,
    )

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=_with_spark(_bronze),
    )

    silver = PythonOperator(
        task_id="silver_conform",
        python_callable=_with_spark(_silver),
    )

    silver_gate = PythonOperator(
        task_id="silver_quality_gate",
        python_callable=_with_spark(_gate("silver")),
    )

    gold = PythonOperator(
        task_id="gold_merge_fact",
        python_callable=_with_spark(_gold),
        # See the module docstring: retrying work whose input a gate rejected
        # only reproduces the same failure.
        retries=0,
    )

    gold_gate = PythonOperator(
        task_id="gold_quality_gate",
        python_callable=_with_spark(_gate("gold")),
        retries=0,
    )

    fetch >> bronze >> silver >> silver_gate >> gold >> gold_gate
