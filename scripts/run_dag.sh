#!/usr/bin/env bash
# Initialise Airflow and run the DAG once, without starting the scheduler.
#
# `airflow dags test` executes the DAG in-process and prints each task's state.
# It is the right way to demonstrate this locally: `airflow standalone` starts a
# scheduler, a triggerer, a webserver and a DAG processor, which together want
# more memory than this laptop has spare - and none of that is needed to prove
# the DAG works.
set -uo pipefail

PROJ=/mnt/d/projects/Data_Engineer/spark-delta-lakehouse
cd "$PROJ"

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export AIRFLOW_HOME="$PROJ/.airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$PROJ/dags"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$PROJ/.airflow/airflow.db"
export LAKEHOUSE_ROOT="${LAKEHOUSE_ROOT:-/root/lakehouse}"
export PYTHONPATH="$PROJ"
export PYSPARK_PYTHON="$PROJ/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJ/.venv/bin/python"
# The warehouse lives on WSL-native ext4; the Airflow metadata DB cannot, so it
# goes under the project on /mnt/d.
mkdir -p "$AIRFLOW_HOME"

echo "=== airflow version ==="
.venv/bin/airflow version

echo "=== init db (idempotent) ==="
.venv/bin/airflow db init 2>&1 | tail -3

echo
echo "=== import check: does the DAG parse? ==="
.venv/bin/airflow dags list 2>&1 | grep -E "lakehouse_medallion|Invalid|Traceback" | head -5

if [ "${1:-}" = "--init-only" ]; then
    echo "init only; stopping before the run"
    exit 0
fi

echo
echo "=== running the DAG (fetch -> bronze -> silver -> gate -> gold -> gate) ==="
.venv/bin/airflow dags test lakehouse_medallion "$(date -u +%Y-%m-%dT00:00:00+00:00)" 2>&1 \
  | grep -vE 'WARN |INFO |log4j|SLF4J|^\s*$|^\[Stage|Ivy|jars for|confs:|downloading|SUCCESSFUL|found |artifacts copied|bogus|resolving|resolution report|modules in use|from central|retrieving'