"""REAL scheduled-vs-actual delay analysis on the public MBTA (Boston) bus dataset.

WHY: BMTC publishes only a static timetable, so BMTC delay cannot be measured. This module runs the
same Spark approach (explicit schema, cleaning waterfall, window functions, aggregations) on a dataset
that has BOTH the scheduled and the actual time of every bus at every timepoint.
All results are about BOSTON, never Bengaluru.

Dataset quirks handled (checked on the real files):
  * `scheduled` / `actual` are time-of-day stamped 1900-01-01T..Z, so a trip after midnight wraps to 00:xx.
    delay = actual - scheduled is therefore computed modulo 24 h, mapped into [-12 h, +12 h).
  * 6.5% of rows have no actual time (data collection gaps) -> dropped and counted.
  * Only trips with standard_type = 'Schedule' are judged on schedule (the data dictionary says scheduled
    time must NOT be used for reliability on 'Headway' trips); 'Headway' trips are analysed via headways.
CLI:  python -m src.delay_pipeline
"""
import argparse
import json
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src import features, ingest
from src.clean import Waterfall
from src.pipeline import save_table

COLS = ["service_date", "route_id", "direction_id", "half_trip_id", "stop_id", "time_point_id",
        "time_point_order", "point_type", "standard_type", "scheduled", "actual", "scheduled_headway", "headway"]
SCHEMA = StructType([StructField(c, StringType(), True) for c in COLS])   # all text, cast explicitly below


def iso_to_sec(col):
    """'1900-01-01T06:05:03Z' -> seconds since midnight (null if not in that shape)."""
    t = F.regexp_extract(col, r"T(\d{2}):(\d{2}):(\d{2})", 0)
    parts = F.split(F.substring(t, 2, 8), ":")
    ok = col.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    return F.when(ok, parts.getItem(0).cast("int") * 3600 + parts.getItem(1).cast("int") * 60
                  + parts.getItem(2).cast("int"))


def wrap_delay(actual_sec, sched_sec):
    """actual - scheduled modulo 24 h, mapped to [-43200, 43200) so 23:59 vs 00:01 is +2 min."""
    return F.pmod(actual_sec - sched_sec + 43200, F.lit(86400)) - 43200


def input_files(dcfg) -> list:
    files = [str(Path(dcfg["raw_dir"]) / dcfg["file_pattern"].format(month=m)) for m in dcfg["months"]]
    missing = [f for f in files if not Path(f).exists()]
    if missing:
        raise SystemExit(f"missing delay data files {missing}; run scripts/download_delay_data.py")
    return files


# ------------------------------------------------------------------ stage 1: ingest + clean
def stage_delay_clean(spark, cfg, out_path: str, wf: Optional[Waterfall] = None):
    wf = wf or Waterfall(enabled=False)
    d = cfg["delay"]
    raw = (spark.read.option("header", True).schema(SCHEMA).csv(input_files(d)))
    for c in COLS:                                             # trim, blank -> null
        t = F.trim(F.col(c))
        raw = raw.withColumn(c, F.when(F.length(t) > 0, t))
    wf.start("1. raw arrival/departure rows", raw, "rows read from the monthly CSVs")
    df = raw.filter(F.col("service_date").isNotNull() & F.col("route_id").isNotNull()
                    & F.col("half_trip_id").isNotNull() & F.col("time_point_order").isNotNull())
    wf.step("2. drop null service_date/route/trip/order", df, "rows cannot be placed in a trip")
    df = df.dropDuplicates(["service_date", "half_trip_id", "time_point_order"])
    wf.step("3. drop duplicate (date, trip, timepoint order)", df, "duplicate events")
    df = (df.withColumn("sched_sec", iso_to_sec(F.col("scheduled")))
            .withColumn("act_sec", iso_to_sec(F.col("actual")))
            .withColumn("time_point_order", F.col("time_point_order").cast("int"))
            .withColumn("service_date", F.to_date("service_date"))
            .withColumn("scheduled_headway", F.col("scheduled_headway").cast("int"))
            .withColumn("headway", F.col("headway").cast("int")))
    df = df.filter(F.col("sched_sec").isNotNull())
    wf.step("4. drop rows without a valid scheduled time", df, "scheduled time missing/unparseable")
    df = df.filter(F.col("act_sec").isNotNull())
    wf.step("5. drop rows without an actual time", df, "no actual time recorded (data-collection gaps)")
    df = df.withColumn("delay_sec", wrap_delay(F.col("act_sec"), F.col("sched_sec")))
    df = df.filter(F.abs("delay_sec") <= d["max_abs_delay_sec"])
    wf.step(f"6. drop |delay| > {d['max_abs_delay_sec']} s", df, "implausible delay (likely data error)")
    df = df.select("service_date", "route_id", "direction_id", "half_trip_id", "stop_id", "time_point_id",
                   "time_point_order", "point_type", "standard_type", "sched_sec", "act_sec", "delay_sec",
                   "scheduled_headway", "headway")
    df.write.mode("overwrite").parquet(out_path)


# ------------------------------------------------------------------ stage 2: window + aggregation
def _hour(sec):
    return (F.floor(sec / 3600) % 24).cast("int")


def stage_delay_agg(spark, cfg, clean_path: str, tables_dir: str, wf: Optional[Waterfall] = None):
    wf = wf or Waterfall(enabled=False)
    d, minn = cfg["delay"], cfg["delay"]["min_events"]
    lo, hi = d["on_time_early_sec"], d["on_time_late_sec"]
    ev = spark.read.parquet(clean_path)
    ev = (ev.withColumn("hour_of_day", _hour(F.col("sched_sec")))
            .withColumn("time_bucket", features.time_bucket_expr(F.col("hour_of_day"), cfg))
            .withColumn("period", features.period_expr(F.col("time_bucket")))
            .withColumn("day_type", F.when(F.dayofweek("service_date").isin(1, 7), "weekend").otherwise("weekday")))
    sched = ev.filter(F.col("standard_type") == "Schedule")
    wf.step("7. keep schedule-standard trips", sched,
            "scope: delay is only valid for 'Schedule' trips; 'Headway' trips are judged on headways",
            kind="scope")
    sched = sched.cache()
    stats = [F.count("*").alias("n"), F.avg("delay_sec").alias("mean_delay_sec"),
             F.percentile_approx("delay_sec", 0.5).alias("median_delay_sec"),
             F.percentile_approx("delay_sec", 0.9).alias("p90_delay_sec"),
             F.stddev_samp("delay_sec").alias("std_delay_sec"),
             F.avg((F.col("delay_sec").between(lo, hi)).cast("int")).alias("on_time_rate"),
             F.avg((F.col("delay_sec") > hi).cast("int")).alias("late_rate"),
             F.avg((F.col("delay_sec") < lo).cast("int")).alias("early_rate")]
    T = tables_dir

    # overall + breakdowns
    save_table(sched.agg(*stats), "delay_overall", T)
    save_table(sched.groupBy("route_id").agg(*stats).filter(F.col("n") >= minn), "delay_by_route", T,
               order_by=[F.desc("mean_delay_sec")])
    save_table(sched.groupBy("hour_of_day").agg(*stats), "delay_by_hour", T, order_by=["hour_of_day"])
    save_table(sched.groupBy("day_type", "period").agg(*stats), "delay_by_daytype_period", T)
    save_table(sched.groupBy("point_type").agg(*stats), "delay_by_point_type", T)
    save_table(sched.groupBy("time_point_order").agg(*stats).filter(F.col("n") >= minn),
               "delay_by_timepoint_order", T, order_by=["time_point_order"])
    pk = (sched.groupBy("route_id", "direction_id").pivot("period", ["PEAK", "OFF_PEAK"])
          .agg(F.count("*").alias("n"), F.avg("delay_sec").alias("mean_delay_sec"),
               F.avg(F.col("delay_sec").between(lo, hi).cast("int")).alias("on_time_rate")))
    save_table(pk.filter((F.col("PEAK_n") >= minn) & (F.col("OFF_PEAK_n") >= minn))
               .withColumn("peak_minus_off_delay_sec", F.col("PEAK_mean_delay_sec") - F.col("OFF_PEAK_mean_delay_sec")),
               "delay_peak_vs_offpeak_by_route", T, order_by=[F.desc("peak_minus_off_delay_sec")])
    sched.sample(False, 0.02, seed=7).select("delay_sec", "time_bucket", "hour_of_day", "day_type").toPandas() \
        .to_parquet(Path(T) / "delay_sample_2pct.parquet", index=False)

    # window: per-trip lag over consecutive timepoints -> scheduled vs ACTUAL hop (run) time
    w = Window.partitionBy("service_date", "half_trip_id").orderBy("time_point_order")
    hop = (sched.withColumn("from_tp", F.lag("time_point_id").over(w))
           .withColumn("prev_sched", F.lag("sched_sec").over(w))
           .withColumn("prev_act", F.lag("act_sec").over(w))
           .filter(F.col("from_tp").isNotNull())
           .withColumn("sched_hop_sec", F.pmod(F.col("sched_sec") - F.col("prev_sched"), F.lit(86400)))
           .withColumn("act_hop_sec", F.pmod(F.col("act_sec") - F.col("prev_act"), F.lit(86400)))
           .filter((F.col("sched_hop_sec") > 0) & (F.col("sched_hop_sec") < 7200) & (F.col("act_hop_sec") > 0)
                   & (F.col("act_hop_sec") < 14400))
           .withColumn("added_sec", F.col("act_hop_sec") - F.col("sched_hop_sec")))
    hop_stats = [F.count("*").alias("n"), F.avg("sched_hop_sec").alias("mean_sched_hop"),
                 F.avg("act_hop_sec").alias("mean_act_hop"), F.avg("added_sec").alias("mean_added_sec")]
    save_table(hop.agg(*hop_stats, F.stddev_samp("sched_hop_sec").alias("std_sched_hop"),
                       F.stddev_samp("act_hop_sec").alias("std_act_hop")), "delay_hop_overall", T)
    seg = (hop.groupBy("route_id", "direction_id", "from_tp", "time_point_id")
           .pivot("period", ["PEAK", "OFF_PEAK"]).agg(*hop_stats))
    seg = (seg.withColumn("sched_slowdown_ratio", F.col("PEAK_mean_sched_hop") / F.col("OFF_PEAK_mean_sched_hop"))
              .withColumn("actual_slowdown_ratio", F.col("PEAK_mean_act_hop") / F.col("OFF_PEAK_mean_act_hop"))
              .filter((F.col("PEAK_n") >= minn) & (F.col("OFF_PEAK_n") >= minn)))
    save_table(seg, "delay_segment_slowdown", T, parquet_full=True)
    save_table(seg.orderBy(F.desc("actual_slowdown_ratio")), "delay_top_segments_actual_slowdown", T, 200,
               [F.desc("actual_slowdown_ratio")])
    save_table(hop.groupBy("hour_of_day").agg(*hop_stats), "delay_hop_by_hour", T, order_by=["hour_of_day"])
    hop.sample(False, 0.02, seed=7).select("sched_hop_sec", "act_hop_sec", "period").toPandas() \
        .to_parquet(Path(T) / "delay_hop_sample_2pct.parquet", index=False)
    sched.unpersist()

    # headway adherence on Headway-standard trips (bunching / gaps)
    hw = (ev.filter((F.col("standard_type") == "Headway") & F.col("headway").isNotNull()
                    & (F.col("scheduled_headway") > 0))
          .withColumn("ratio", F.col("headway") / F.col("scheduled_headway"))
          .filter((F.col("headway") >= 0) & (F.col("ratio") < 10)))
    hstats = [F.count("*").alias("n"), F.avg("ratio").alias("mean_headway_ratio"),
              F.avg((F.col("ratio") < d["headway_bunch_ratio"]).cast("int")).alias("bunched_rate"),
              F.avg((F.col("ratio") > d["headway_gap_ratio"]).cast("int")).alias("gap_rate")]
    save_table(hw.agg(*hstats), "headway_overall", T)
    save_table(hw.groupBy("route_id").agg(*hstats).filter(F.col("n") >= minn), "headway_by_route", T,
               order_by=[F.desc("bunched_rate")])
    save_table(hw.groupBy("hour_of_day").agg(*hstats), "headway_by_hour", T, order_by=["hour_of_day"])


def run_full(spark, cfg, tables_dir, interim_dir):
    wf = Waterfall()
    t0 = time.perf_counter()
    stage_delay_clean(spark, cfg, f"{interim_dir}/events", wf)
    stage_delay_agg(spark, cfg, f"{interim_dir}/events", tables_dir, wf)
    wf.extras["pipeline_seconds_with_counting"] = round(time.perf_counter() - t0, 1)
    wf.extras["months"] = cfg["delay"]["months"]
    return wf


def main():
    ap = argparse.ArgumentParser(description="Real scheduled-vs-actual delay analysis (Boston MBTA bus data)")
    ap.add_argument("--cores", type=int, default=4)
    a = ap.parse_args()
    cfg = ingest.load_config()
    sp = cfg["spark"]
    spark = ingest.get_spark(a.cores, sp["shuffle_partitions"], "6g", sp["adaptive_enabled"], app="delay")
    tdir = cfg["paths"]["tables_dir"]
    wf = run_full(spark, cfg, tdir, cfg["delay"]["interim_dir"])
    wf.save(tdir, "delay_waterfall")
    print(wf.to_pandas()[["stage", "rows_in", "rows_out", "pct_retained"]].to_string(index=False))


if __name__ == "__main__":
    main()
