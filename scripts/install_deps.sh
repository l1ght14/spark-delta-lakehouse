#!/usr/bin/env bash
# Idempotent dependency install for the lakehouse project.
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

PROJ=/mnt/d/projects/Data_Engineer/spark-delta-lakehouse
cd "$PROJ"

CONSTRAINTS="$PROJ/constraints-3.12.txt"
AIRFLOW_VER=2.10.5

echo "=== fetching Airflow $AIRFLOW_VER constraints for py3.12 ==="
curl -sSf -o "$CONSTRAINTS" \
  "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VER}/constraints-3.12.txt"
echo "    $(wc -l < "$CONSTRAINTS") pins"

echo "=== installing airflow (constrained) ==="
uv pip install --python .venv/bin/python \
    "apache-airflow==$AIRFLOW_VER" \
    --constraint "$CONSTRAINTS" 2>&1 | tail -4

echo "=== installing spark + delta (constrained) ==="
# Installed in a second step because the constraint file pins pyspark as a
# transitive dependency of the spark provider; installing them together with
# airflow makes the resolver choose versions that Airflow's own pins reject.
uv pip install --python .venv/bin/python \
    pyspark==3.5.4 delta-spark==3.3.0 \
    --constraint "$CONSTRAINTS" 2>&1 | tail -4

echo "=== versions ==="
.venv/bin/python - <<'PY'
import importlib.metadata as md
for p in ("apache-airflow", "pyspark", "delta-spark", "delta"):
    try:
        print(f"  {p:<18} {md.version(p)}")
    except md.PackageNotFoundError:
        print(f"  {p:<18} (not installed)")
PY

echo "=== airflow CLI ==="
export AIRFLOW_HOME="$PROJ/.airflow"
.venv/bin/airflow version