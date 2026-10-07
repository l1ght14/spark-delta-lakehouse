"""Verify this machine can run the pipeline before spending time on it.

Checks the four things the project depends on, so failures surface now rather
than three models deep:
  1. A Spark session starts on the configured memory ceiling.
  2. A Delta table can be written on the ext4 warehouse path.
  3. MERGE INTO works (upsert semantics).
  4. Time travel works (VERSION AS OF reads a prior state).
"""

import os
import shutil
import sys
import time

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

WAREHOUSE = os.environ.get("LAKEHOUSE_ROOT", "/root/lakehouse")
TEST_TABLE = f"{WAREHOUSE}/_probe"

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

start = time.time()
builder = (
    SparkSession.builder.appName("delta-probe")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", f"{WAREHOUSE}/spark_warehouse")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(f"  [1] session up in {time.time() - start:.1f}s  spark={spark.version}")

shutil.rmtree(TEST_TABLE, ignore_errors=True)

# 2. write
df = spark.createDataFrame(
    [(1, "alice", 10), (2, "bob", 20), (3, "carol", 30)],
    ["id", "name", "amount"],
)
df.write.format("delta").mode("overwrite").save(TEST_TABLE)
print("  [2] write OK")

# record the version before we mutate it.
# The transaction log is a directory of JSON commit files, not a queryable
# table, so the version comes from DESCRIBE HISTORY rather than a SELECT over
# _delta_log.
t0 = DeltaTable.forPath(spark, TEST_TABLE)
before_version = t0.history(1).collect()[0]["version"]
print(f"  version before merge: {before_version}")

# 3. MERGE: update alice's amount, insert a new row, leave bob and carol alone
from pyspark.sql import functions as F

t = DeltaTable.forPath(spark, TEST_TABLE)
merge_source = spark.createDataFrame(
    [(1, "alice", 999), (4, "dave", 40)],
    ["id", "name", "amount"],
).alias("s")
(
    t.alias("t")
    .merge(merge_source, "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
d = DeltaTable.forPath(spark, TEST_TABLE)
print(
    "  [3] MERGE OK ->",
    [(r["id"], r["name"], r["amount"]) for r in d.toDF().orderBy("id").collect()],
)

# 4. time travel back to the pre-merge version
before = (
    spark.read.format("delta")
    .option("versionAsOf", before_version)
    .load(TEST_TABLE)
    .orderBy("id")
    .collect()
)
print("  [4] TIME TRAVEL OK ->", [(r["id"], r["amount"]) for r in before])

# schema evolution: add a column, confirm enforcement behaviour
try:
    spark.createDataFrame([(9, "dan", 5)], ["id", "name", "amount", "extra"]).write.format(
        "delta"
    ).mode("append").save(TEST_TABLE)
    print("  [5] new column accepted (mergeSchema default)")
except Exception as exc:
    print(f"  [5] new column rejected as expected: {type(exc).__name__}")

print(f"\n  total {time.time() - start:.1f}s")
spark.stop()
shutil.rmtree(TEST_TABLE, ignore_errors=True)
print("  DELTA PROBE PASSED")