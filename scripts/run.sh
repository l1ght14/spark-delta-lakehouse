#!/usr/bin/env bash
# Run a python entrypoint in the project venv, stripping Spark's log noise.
set -uo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJ"

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export LAKEHOUSE_ROOT="${LAKEHOUSE_ROOT:-$HOME/lakehouse}"

# Workers must run the SAME interpreter as the driver. Ubuntu 26.04 ships
# python3.14 system-wide while the venv is 3.12 (pinned for Airflow), so
# without this PySpark aborts with PYTHON_VERSION_MISMATCH on the first task
# rather than at startup - which makes it look like a Delta problem.
export PYSPARK_PYTHON="$PROJ/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJ/.venv/bin/python"

# Scripts under scripts/ import `src.lakehouse.*`; running a file by path puts
# scripts/ on sys.path, not the project root.
export PYTHONPATH="$PROJ"

echo "WSL memory: $(free -m | awk '/^Mem:/{print $7" MB available of "$2}')"

"$PROJ/.venv/bin/python" "$@" 2>&1 \
  | grep -vE 'WARN |INFO |log4j|SLF4J|Setting default log level|Using Spark|^\s+at |NativeCodeLoader|Ivy Default Cache|jars for the packages|^\s*(confs|downloading|\[SUCCESSFUL|found |[0-9]+ artifacts copied)|^\s+\|[-+ ]|bogus'