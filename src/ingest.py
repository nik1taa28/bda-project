"""Ingest: explicit schemas (T2, never inferSchema) + readers for the GTFS .txt files.

Spark's CSV reader matches a user schema to columns BY POSITION, so every schema
below lists ALL columns of the real file in file order; read_gtfs() also checks
the header line so a changed feed fails loudly instead of silently mis-aligning.
"""
import os
import sys
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)


def _schema(*fields):
    return StructType([StructField(n, t, True) for n, t in fields])


S, I, D = StringType(), IntegerType(), DoubleType()

SCHEMAS = {
    "routes": _schema(("route_id", S), ("agency_id", S), ("route_short_name", S),
                      ("route_long_name", S), ("route_desc", S), ("route_type", I),
                      ("route_url", S)),
    "trips": _schema(("route_id", S), ("service_id", S), ("trip_id", S),
                     ("trip_headsign", S), ("trip_short_name", S), ("direction_id", I),
                     ("shape_id", S), ("wheelchair_accessible", I), ("bikes_allowed", I)),
    "stop_times": _schema(("trip_id", S), ("arrival_time", S), ("departure_time", S),
                          ("stop_id", S), ("stop_sequence", I),
                          ("shape_dist_traveled", D), ("timepoint", I)),
    "stops": _schema(("stop_id", S), ("stop_code", S), ("stop_name", S), ("stop_desc", S),
                     ("stop_lat", D), ("stop_lon", D), ("zone_id", S), ("stop_url", S),
                     ("location_type", I), ("parent_station", S), ("platform_code", S),
                     ("wheelchair_boarding", I)),
    "calendar": _schema(("service_id", S), ("monday", I), ("tuesday", I), ("wednesday", I),
                        ("thursday", I), ("friday", I), ("saturday", I), ("sunday", I),
                        ("start_date", S), ("end_date", S)),
    "calendar_dates": _schema(("service_id", S), ("date", S), ("exception_type", I)),
}
OPTIONAL = {"calendar_dates"}   # D2: only read optional files if they exist


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_spark(cores: int = 4, shuffle_partitions: int = 8, driver_memory: str = "4g",
              adaptive: bool = False, broadcast_threshold: Optional[int] = None,
              extra: Optional[dict] = None, app: str = "bmtc-gtfs") -> SparkSession:
    """One SparkSession per Python process (T15: master cannot change in-process)."""
    # make Python workers use the same interpreter as the driver (avoids version-mismatch errors)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    b = (SparkSession.builder.master(f"local[{cores}]").appName(app)
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.driver.memory", driver_memory)
         .config("spark.sql.adaptive.enabled", str(adaptive).lower())
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


def _tidy_strings(df: DataFrame) -> DataFrame:
    """Trim whitespace on string columns; blank strings become null."""
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            t = F.trim(F.col(f.name))
            df = df.withColumn(f.name, F.when(F.length(t) > 0, t))
    return df


def gtfs_path(raw_dir: str, name: str) -> Path:
    return Path(raw_dir) / f"{name}.txt"


def gtfs_exists(raw_dir: str, name: str) -> bool:
    return gtfs_path(raw_dir, name).exists()


def read_gtfs(spark: SparkSession, raw_dir: str, name: str) -> DataFrame:
    path = gtfs_path(raw_dir, name)
    header = path.open(encoding="utf-8-sig").readline().strip().split(",")
    expected = SCHEMAS[name].fieldNames()
    if header != expected:
        raise ValueError(f"{path.name}: header {header} != expected {expected}. "
                         f"The feed layout changed; update SCHEMAS in ingest.py.")
    df = (spark.read.option("header", True).option("escape", '"')
          .option("mode", "PERMISSIVE").schema(SCHEMAS[name]).csv(str(path)))
    return _tidy_strings(df)


def read_all(spark: SparkSession, raw_dir: str) -> dict:
    out = {n: read_gtfs(spark, raw_dir, n)
           for n in ("routes", "trips", "stop_times", "stops", "calendar")}
    if gtfs_exists(raw_dir, "calendar_dates"):
        out["calendar_dates"] = read_gtfs(spark, raw_dir, "calendar_dates")
    return out
