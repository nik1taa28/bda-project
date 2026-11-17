"""Delay pipeline tests: time parsing, midnight wrap, cleaning, and a pandas cross-check on real data (T22-style)."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import functions as F

from src import delay_pipeline as dp
from src.clean import Waterfall

ROOT = Path(__file__).resolve().parent.parent


def test_iso_to_sec_and_wrap(spark):
    df = spark.createDataFrame([("1900-01-01T06:05:03Z", "1900-01-01T06:05:00Z"),
                                ("1900-01-01T00:01:00Z", "1900-01-01T23:59:00Z"),   # after-midnight actual
                                ("1900-01-01T23:58:00Z", "1900-01-01T00:02:00Z"),   # early across midnight
                                ("bad", "1900-01-01T06:00:00Z")], ["a", "s"])
    r = df.select(dp.wrap_delay(dp.iso_to_sec(F.col("a")), dp.iso_to_sec(F.col("s"))).alias("d")).collect()
    assert [x["d"] for x in r] == [3, 120, -240, None]     # +3 s, +2 min across midnight, -4 min, null


def test_delay_clean_rules(spark, cfg, tmp_path):
    rows = ["service_date,route_id,direction_id,half_trip_id,stop_id,time_point_id,time_point_order,point_type,standard_type,scheduled,actual,scheduled_headway,headway",
            "2024-01-01,01,Inbound,1,10,a,1,Startpoint,Schedule,1900-01-01T06:00:00Z,1900-01-01T06:01:00Z,,",
            "2024-01-01,01,Inbound,1,10,a,1,Startpoint,Schedule,1900-01-01T06:00:00Z,1900-01-01T06:01:00Z,,",   # duplicate
            "2024-01-01,01,Inbound,1,11,b,2,Midpoint,Schedule,1900-01-01T06:10:00Z,,,,",                          # no actual
            "2024-01-01,01,Inbound,1,12,c,3,Midpoint,Schedule,1900-01-01T06:20:00Z,1900-01-01T09:30:00Z,,",       # 3.2 h late: error
            "2024-01-01,01,Inbound,1,13,d,4,Endpoint,Schedule,1900-01-01T23:58:00Z,1900-01-01T00:01:00Z,,",       # wraps midnight (+3 min)
            ",01,Inbound,1,14,e,5,Endpoint,Schedule,1900-01-01T06:00:00Z,1900-01-01T06:00:00Z,,"]                # null date
    d = tmp_path / "raw"; d.mkdir()
    (d / "MBTA-Bus-Arrival-Departure-Times_2024-01.csv").write_text("\n".join(rows) + "\n")
    c = {**cfg, "delay": {**cfg["delay"], "raw_dir": str(d), "months": [1]}}
    wf = Waterfall()
    out = str(tmp_path / "events")
    dp.stage_delay_clean(spark, c, out, wf)
    got = spark.read.parquet(out).collect()
    assert [(r["time_point_order"], r["delay_sec"]) for r in sorted(got, key=lambda r: r["time_point_order"])] == [(1, 60), (4, 180)]
    assert [r["rows_out"] for r in wf.rows] == [6, 5, 4, 4, 3, 2]


def test_spark_matches_pandas_on_real_data(spark, cfg):
    ev_p = ROOT / cfg["delay"]["interim_dir"] / "events"
    raw = ROOT / cfg["delay"]["raw_dir"] / cfg["delay"]["file_pattern"].format(month=1)
    if not ev_p.exists() or not raw.exists():
        pytest.skip("run `python -m src.delay_pipeline` first")
    lo, hi = cfg["delay"]["on_time_early_sec"], cfg["delay"]["on_time_late_sec"]
    # independent pandas computation straight from the raw CSV (January, 6 routes)
    routes = ["01", "28", "39", "66", "111", "CT2"]
    raw_df = pd.read_csv(raw, dtype=str)
    raw_df = raw_df[raw_df.route_id.isin(routes)].dropna(subset=["service_date", "route_id", "half_trip_id", "time_point_order"])
    raw_df = raw_df.drop_duplicates(["service_date", "half_trip_id", "time_point_order"])
    raw_df = raw_df.dropna(subset=["scheduled", "actual"])
    raw_df = raw_df[raw_df.standard_type == "Schedule"]
    s = pd.to_datetime(raw_df.scheduled, utc=True)
    a = pd.to_datetime(raw_df.actual, utc=True)
    d = ((a - s).dt.total_seconds() + 43200) % 86400 - 43200
    raw_df = raw_df.assign(delay=d)
    raw_df = raw_df[raw_df.delay.abs() <= cfg["delay"]["max_abs_delay_sec"]]
    exp = raw_df.groupby("route_id").delay.agg(n="count", mean="mean", std="std").join(
        raw_df.assign(ot=raw_df.delay.between(lo, hi)).groupby("route_id").ot.mean().rename("on_time"))
    # Spark result (from the pipeline's cleaned parquet), same slice
    sp = spark.read.parquet(str(ev_p)).filter(F.col("route_id").isin(routes) & (F.col("standard_type") == "Schedule")
                                              & (F.month("service_date") == 1))
    got = (sp.groupBy("route_id").agg(F.count("*").alias("n"), F.avg("delay_sec").alias("mean"),
                                      F.stddev_samp("delay_sec").alias("std"),
                                      F.avg(F.col("delay_sec").between(lo, hi).cast("int")).alias("on_time"))
           .toPandas().set_index("route_id").loc[exp.index])
    assert (got.n == exp.n).all()
    worst = max(float((got["mean"] - exp["mean"]).abs().max()), float((got["std"] - exp["std"]).abs().max()),
                float((got.on_time - exp.on_time).abs().max()))
    assert worst < 1e-6
    out = ROOT / "outputs" / "delay_correctness_check.json"
    out.write_text(json.dumps({"routes_checked": routes, "events_compared": int(exp.n.sum()),
                               "max_abs_diff_mean_std_on_time": worst, "passed": True}, indent=2))
