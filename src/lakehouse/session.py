"""The one place that knows how to build a Spark session.

Every entrypoint goes through get_spark(). Centralising it means the Delta
configuration, the memory ceiling and the local-runner settings exist once,
rather than being re-stated in each script and drifting apart.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from . import config


def get_spark(app_name: str = "lakehouse") -> SparkSession:
    """Return a SparkSession with Delta Lake wired in.

    Reuses an existing session if one is live, so calling this twice in a
    process is cheap rather than starting a second 25-second JVM.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    config.ensure_dirs()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.MASTER)
        # Both of these are required. The extension provides MERGE, time travel
        # and the Delta catalogue; the catalog setting is what makes
        # `delta.\`path\`` and the DeltaTable API resolve.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(config.LAKEHOUSE_ROOT / "spark_warehouse"))
        .config("spark.driver.memory", config.DRIVER_MEMORY)
        # 4 partitions, not the default 200. At 100k rows, 200 tasks is 200
        # JVM task launches to move almost no data.
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        # No UI: it costs memory we do not have, and nothing here needs it.
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def stop_spark(spark: SparkSession) -> None:
    spark.stop()
