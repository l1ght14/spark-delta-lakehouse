#!/usr/bin/env bash
# Test entrypoint.
#   ./scripts/test.sh          fast tier only (no Spark, ~2s)
#   ./scripts/test.sh slow     fast + Spark integration tier (~3min)
set -uo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJ"

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export LAKEHOUSE_ROOT="${LAKEHOUSE_ROOT:-$HOME/lakehouse}"
export PYTHONPATH="$PROJ"
export PYSPARK_PYTHON="$PROJ/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJ/.venv/bin/python"

if [ "${1:-}" = "slow" ]; then
    exec "$PROJ/.venv/bin/python" -m pytest tests -v
fi

exec "$PROJ/.venv/bin/python" -m pytest tests -v -m 'not slow'