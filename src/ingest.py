"""Spark session and configuration loading."""
import os
import sys
from typing import Optional

import yaml
from pyspark.sql import SparkSession


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_spark(cores: int = 4, shuffle_partitions: int = 8, driver_memory: str = "4g",
              adaptive: bool = False, broadcast_threshold: Optional[int] = None,
              extra: Optional[dict] = None, app: str = "delay") -> SparkSession:
    """One SparkSession per Python process (T15: master cannot change in-process)."""
    # make Python workers use the same interpreter as the driver (avoids version-mismatch errors)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    b = (SparkSession.builder.master(f"local[{cores}]").appName(app)
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.driver.memory", driver_memory)
         .config("spark.sql.adaptive.enabled", str(adaptive).lower())
         .config("spark.driver.bindAddress", "127.0.0.1")   # local mode: do not depend on the network being up
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.session.timeZone", "UTC"))
    if broadcast_threshold is not None:
        b = b.config("spark.sql.autoBroadcastJoinThreshold", str(broadcast_threshold))
    for k, v in (extra or {}).items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
