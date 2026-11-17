"""T1: GTFS times beyond 24:00:00 must parse (unix_timestamp would return null)."""
import pytest
from pyspark.sql import functions as F

from src.gtfs_time import gtfs_time_to_sec, gtfs_time_to_sec_py, hour_of_day

CASES = [("08:05:00", 29100), ("24:00:00", 86400), ("26:30:15", 95415), ("25:10:00", 90600),
         ("8:05:00", 29100), (" 09:00:00 ", 32400)]
BAD = ["", "abc", "12:60:00", "12:00:61", "12:00", "1234:00:00", None]


def test_spark_parser_valid(spark):
    df = spark.createDataFrame([(a,) for a, _ in CASES], ["t"])
    got = {r["t"]: r["s"] for r in df.select("t", gtfs_time_to_sec(F.col("t")).alias("s")).collect()}
    for text, sec in CASES:
        assert got[text] == sec


def test_spark_parser_invalid_is_null(spark):
    df = spark.createDataFrame([(b,) for b in BAD], "t string")
    assert df.select(gtfs_time_to_sec(F.col("t")).alias("s")).filter("s is not null").count() == 0


def test_python_twin_matches():
    for text, sec in CASES:
        assert gtfs_time_to_sec_py(text) == sec
    assert all(gtfs_time_to_sec_py(b) is None for b in BAD)


def test_hour_of_day_wraps_past_midnight(spark):
    df = spark.createDataFrame([(29100,), (86400,), (95415,)], ["s"])
    assert [r[0] for r in df.select(hour_of_day(F.col("s"))).collect()] == [8, 0, 2]


def test_unix_timestamp_would_have_failed(spark):
    """Documents WHY the custom parser exists."""
    df = spark.createDataFrame([("26:30:15",)], ["t"])
    assert df.select(F.unix_timestamp("t", "HH:mm:ss")).first()[0] is None
