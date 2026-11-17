"""PySpark pipeline: joins, window functions and aggregations.  (Pranavi's area)

Three stages, each ends with a Parquet write (an action) so it can be timed alone:
  stage_clean       read stop_times -> clean -> parquet
  stage_join        stop_times JOIN trips JOIN routes JOIN stops (+ service-day filter)
  stage_window_agg  lag() window per trip -> hops -> segment / route / headway tables

CLI:  python -m src.pipeline --service-day weekday
"""
import argparse
import json
import shutil
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from src import features, ingest
from src.clean import Waterfall, clean_dims, clean_stop_times
from src.gtfs_time import hour_of_day

SEG_KEYS = ["route_id", "route_short_name", "route_class", "direction_id",
            "from_stop_id", "to_stop_id", "from_stop_name", "to_stop_name"]


def _b(df: DataFrame, use_broadcast: bool) -> DataFrame:
    """T19: explicit broadcast hint on the small dimension table (or leave it to the planner,
    which with autoBroadcastJoinThreshold=-1 gives a shuffle sort-merge join)."""
    return F.broadcast(df) if use_broadcast else df


# --------------------------------------------------------------------------- stages
def stage_clean(spark, cfg, out_path: str, wf: Optional[Waterfall] = None):
    st = ingest.read_gtfs(spark, cfg["paths"]["raw_dir"], "stop_times")
    clean_stop_times(st, wf).write.mode("overwrite").parquet(out_path)


def select_services(spark, cfg, service_day: str, tables: dict, wf: Waterfall) -> DataFrame:
    """T6: pick the service_ids for the chosen day type from calendar.txt."""
    cal = tables["calendar"]
    if service_day == "weekday":
        cond = ((F.col("monday") == 1) & (F.col("tuesday") == 1) & (F.col("wednesday") == 1)
                & (F.col("thursday") == 1) & (F.col("friday") == 1))
    elif service_day == "weekend":
        cond = (F.col("saturday") == 1) & (F.col("sunday") == 1)
    else:
        raise ValueError("service_day must be weekday or weekend")
    svc = cal.filter(cond).select("service_id").distinct()
    if wf.enabled:
        wf.extras["service_day"] = service_day
        wf.extras["calendar_services_total"] = cal.count()
        wf.extras["calendar_services_selected"] = svc.count()
        wf.extras["calendar_dates_present"] = "calendar_dates" in tables
        if "calendar_dates" in tables:
            wf.extras["calendar_dates_note"] = "present but exceptions not applied to day-type choice"
    return svc


def stage_join(spark, cfg, clean_path: str, out_path: str, service_day: str,
               use_broadcast: bool = True, wf: Optional[Waterfall] = None,
               explain_to: Optional[str] = None):
    wf = wf or Waterfall(enabled=False)
    tables = ingest.read_all(spark, cfg["paths"]["raw_dir"])
    trips, routes, stops = clean_dims(tables)
    svc = select_services(spark, cfg, service_day, tables, wf)

    st = spark.read.parquet(clean_path)
    j = st.join(_b(trips, use_broadcast), "trip_id", "inner")                 # T5
    wf.step("5. join trips", j, "stop_times whose trip_id is not in trips.txt (orphans)")
    j = j.join(_b(svc, use_broadcast), "service_id", "left_semi")             # T6
    wf.step(f"6. keep {service_day} service_ids", j,
            f"scope: only trips whose service runs on the chosen day type ({service_day})",
            kind="scope")
    j = j.join(_b(routes, use_broadcast), "route_id", "inner")
    wf.step("7. join routes", j, "trips whose route_id is not in routes.txt (orphans)")
    j = j.join(_b(stops, use_broadcast), "stop_id", "inner")
    wf.step("8. join stops", j, "stop_times whose stop_id is not in stops.txt (orphans)")
    j = j.filter(F.col("stop_id").isNotNull() & F.col("stop_lat").isNotNull()
                 & F.col("stop_lon").isNotNull())
    wf.step("9. drop null stop_id / stop_lat / stop_lon", j, "stop has no coordinates")
    j = j.select("trip_id", "route_id", "route_short_name", "route_long_name", "service_id",
                 "direction_id", "stop_id", "stop_name", "stop_lat", "stop_lon",
                 "stop_sequence", "arr_sec", "dep_sec", "timepoint", "shape_dist_traveled")
    if explain_to:
        import contextlib, io
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            j.explain(mode="formatted")
        Path(explain_to).parent.mkdir(parents=True, exist_ok=True)
        Path(explain_to).write_text(buf.getvalue())
    j.write.mode("overwrite").parquet(out_path)


# --------------------------------------------------------------------------- aggregations
def _stat_aggs():
    return [F.count("*").alias("n"),
            F.avg("hop_sec").alias("mean_hop_sec"),
            F.stddev_samp("hop_sec").alias("std_hop_sec"),
            F.avg("hop_speed_kmh").alias("mean_speed_kmh"),
            F.percentile_approx("hop_sec", 0.5).alias("p50_hop_sec"),
            F.percentile_approx("hop_sec", 0.9).alias("p90_hop_sec"),
            F.avg("hop_dist_m").alias("mean_dist_m")]


def _cv(std, mean):
    return F.when(mean > 0, std / mean)


def _derive_period_cols(df: DataFrame, mean_col="mean_hop_sec", std_col="std_hop_sec") -> DataFrame:
    """After pivot: CV per period and the peak/off-peak ratios (T14)."""
    for p in ("PEAK", "OFF_PEAK"):
        df = df.withColumn(f"{p}_cv", _cv(F.col(f"{p}_{std_col}"), F.col(f"{p}_{mean_col}")))
    return (df.withColumn("slowdown_ratio", F.col(f"PEAK_{mean_col}") / F.col(f"OFF_PEAK_{mean_col}"))
              .withColumn("speed_ratio_peak_over_off",
                          F.col("PEAK_mean_speed_kmh") / F.col("OFF_PEAK_mean_speed_kmh")))


def _pivot_periods(hops: DataFrame, keys) -> DataFrame:
    return (hops.groupBy(*keys).pivot("period", ["PEAK", "OFF_PEAK"]).agg(*_stat_aggs()))


def save_table(df: DataFrame, name: str, tables_dir: str, csv_rows: Optional[int] = None,
               order_by=None, parquet_full: bool = False):
    """Small tables: one CSV + one Parquet file via pandas. Big tables: Spark Parquet folder."""
    d = Path(tables_dir)
    d.mkdir(parents=True, exist_ok=True)
    if parquet_full:
        p = d / f"{name}.parquet"
        if p.exists():
            shutil.rmtree(p) if p.is_dir() else p.unlink()
        df.write.mode("overwrite").parquet(str(p))
        return
    out = df.orderBy(*order_by) if order_by is not None else df
    if csv_rows:
        out = out.limit(csv_rows)
    pdf = out.toPandas()
    pdf.to_csv(d / f"{name}.csv", index=False)
    pdf.to_parquet(d / f"{name}.parquet", index=False)


def stage_window_agg(spark, cfg, visits_path: str, work_dir: str, tables_dir: str,
                     wf: Optional[Waterfall] = None):
    wf = wf or Waterfall(enabled=False)
    th, minn = cfg["thresholds"], cfg["thresholds"]["min_trips"]
    Path(work_dir).mkdir(parents=True, exist_ok=True)
    visits = spark.read.parquet(visits_path)

    # ---- window: per-trip lag() -> hop features, materialised so later aggregates reuse it
    features.add_hop_features(visits, cfg).write.mode("overwrite").parquet(f"{work_dir}/hops_all")
    hops_all = spark.read.parquet(f"{work_dir}/hops_all")

    # ---- trips + route class (T13)
    trips = features.trip_summary(hops_all, cfg)
    classes = features.classify_routes(trips, cfg).cache()
    trips = trips.join(F.broadcast(classes.select("route_id", "route_class", "route_km")), "route_id")
    trips.write.mode("overwrite").parquet(f"{work_dir}/trips")
    trips = spark.read.parquet(f"{work_dir}/trips")

    # ---- valid hops (T8) with route class
    hops = features.filter_hops(hops_all, cfg, wf)
    hops = hops.join(F.broadcast(classes.select("route_id", "route_class")), "route_id")
    hops.write.mode("overwrite").parquet(f"{work_dir}/hops")
    hops = spark.read.parquet(f"{work_dir}/hops")
    wf.step("15. final analysed hops", hops, "final table used for all statistics", kind="final")

    # ---- Q1: segment level (route, direction, from_stop, to_stop)  (T9, T11, T14)
    seg_bucket = (hops.groupBy(*SEG_KEYS, "time_bucket").agg(*_stat_aggs())
                  .withColumn("cv_hop", _cv(F.col("std_hop_sec"), F.col("mean_hop_sec")))
                  .withColumn("meets_min_trips", F.col("n") >= minn))
    save_table(seg_bucket, "segment_bucket_stats", tables_dir, parquet_full=True)

    seg = _derive_period_cols(_pivot_periods(hops, SEG_KEYS)).cache()
    save_table(seg, "segment_summary", tables_dir, parquet_full=True)
    ok_peak = seg.filter(F.col("PEAK_n") >= minn)
    save_table(ok_peak.filter(F.col("PEAK_cv").isNotNull()), "top_segments_peak_variability",
               tables_dir, 200, [F.desc("PEAK_cv")])
    ok_both = seg.filter((F.col("PEAK_n") >= minn) & (F.col("OFF_PEAK_n") >= minn))
    save_table(ok_both, "top_segments_slowdown", tables_dir, 200, [F.desc("slowdown_ratio")])

    # ---- stop-pair rollup across routes
    sp_keys = ["from_stop_id", "to_stop_id", "from_stop_name", "to_stop_name",
               "from_lat", "from_lon", "to_lat", "to_lon"]
    sp = _derive_period_cols(_pivot_periods(hops, sp_keys))
    n_routes = hops.groupBy("from_stop_id", "to_stop_id").agg(F.countDistinct("route_id").alias("n_routes"))
    sp = sp.join(n_routes, ["from_stop_id", "to_stop_id"]).cache()
    save_table(sp, "stop_pair_summary", tables_dir, parquet_full=True)
    sp_ok = sp.filter((F.col("PEAK_n") >= minn) & (F.col("OFF_PEAK_n") >= minn))
    save_table(sp.filter(F.col("PEAK_n") >= minn).filter(F.col("PEAK_cv").isNotNull()),
               "top_stop_pairs_peak_variability", tables_dir, 200, [F.desc("PEAK_cv")])
    save_table(sp_ok, "top_stop_pairs_slowdown", tables_dir, 200, [F.desc("slowdown_ratio")])

    # per-stop mean slowdown for the optional scatter map
    stop_sl = (sp_ok.groupBy("from_stop_id", "from_stop_name", "from_lat", "from_lon")
               .agg(F.avg("slowdown_ratio").alias("mean_slowdown_ratio"),
                    F.count("*").alias("n_pairs"))
               .withColumnRenamed("from_stop_id", "stop_id").withColumnRenamed("from_stop_name", "stop_name")
               .withColumnRenamed("from_lat", "stop_lat").withColumnRenamed("from_lon", "stop_lon"))
    save_table(stop_sl, "stop_slowdown", tables_dir)

    # ---- per-route trip duration distribution (peak vs off-peak) (Q1, section 6.5)
    r_keys = ["route_id", "route_short_name", "route_class", "route_km", "direction_id"]
    r_aggs = [F.count("*").alias("n"), F.avg("trip_duration_min").alias("mean_min"),
              F.stddev_samp("trip_duration_min").alias("std_min"),
              F.percentile_approx("trip_duration_min", 0.9).alias("p90_min")]
    rt = trips.groupBy(*r_keys).pivot("period", ["PEAK", "OFF_PEAK"]).agg(*r_aggs)
    rt = (rt.withColumn("PEAK_cv", _cv(F.col("PEAK_std_min"), F.col("PEAK_mean_min")))
            .withColumn("OFF_PEAK_cv", _cv(F.col("OFF_PEAK_std_min"), F.col("OFF_PEAK_mean_min")))
            .withColumn("slowdown_ratio", F.col("PEAK_mean_min") / F.col("OFF_PEAK_mean_min")))
    all_r = trips.groupBy(*r_keys).agg(*[a for a in r_aggs]).select(
        *r_keys, F.col("n").alias("all_n"), F.col("mean_min").alias("all_mean_min"),
        F.col("std_min").alias("all_std_min"), F.col("p90_min").alias("all_p90_min"))
    rt = rt.join(all_r, r_keys, "left")
    save_table(rt, "route_summary", tables_dir)
    save_table(rt.filter((F.col("PEAK_n") >= minn) & F.col("PEAK_cv").isNotNull()),
               "top_routes_peak_variability", tables_dir, 200, [F.desc("PEAK_cv")])
    save_table(classes.groupBy("route_class").agg(
        F.count("*").alias("n_routes"), F.round(F.avg("route_km"), 1).alias("mean_route_km"),
        F.round(F.expr("percentile_approx(route_km, 0.5)"), 1).alias("median_route_km")),
        "route_class_counts", tables_dir)
    save_table(classes, "route_classes", tables_dir)
    trips.select("trip_id", "route_id", "route_short_name", "route_class", "direction_id",
                 "start_hour", "time_bucket", "period", "trip_duration_min", "trip_km", "n_stops"
                 ).toPandas().to_parquet(Path(tables_dir) / "trips_summary.parquet", index=False)

    # ---- scheduled headway per route/direction/stop/hour (window over departures)
    wv = Window.partitionBy("route_id", "direction_id", "stop_id").orderBy("dep_sec")
    hw = (visits.withColumn("headway_sec", F.col("dep_sec") - F.lag("dep_sec").over(wv))
          .filter(F.col("headway_sec") > 0)       # 0 = two trips with identical departure; excluded
          .withColumn("hour_of_day", hour_of_day(F.col("dep_sec"))))
    hws = (hw.groupBy("route_id", "route_short_name", "direction_id", "stop_id", "stop_name", "hour_of_day")
           .agg(F.count("*").alias("n_gaps"), F.avg("headway_sec").alias("mean_headway_sec"),
                F.stddev_samp("headway_sec").alias("std_headway_sec"))
           .withColumn("headway_cv", _cv(F.col("std_headway_sec"), F.col("mean_headway_sec")))
           .filter(F.col("n_gaps") >= th["min_headways"]))
    save_table(hws, "headway_by_stop_hour", tables_dir, parquet_full=True)
    hws = spark.read.parquet(str(Path(tables_dir) / "headway_by_stop_hour.parquet"))
    save_table(hws.filter(F.col("headway_cv").isNotNull()), "top_headway_cv", tables_dir, 200,
               [F.desc("headway_cv")])
    save_table(hws.groupBy("hour_of_day").agg(F.count("*").alias("n_groups"),
                                              F.avg("headway_cv").alias("mean_headway_cv"),
                                              F.avg("mean_headway_sec").alias("mean_headway_sec")),
               "headway_hourly_profile", tables_dir, order_by=["hour_of_day"])

    # ---- hourly profile of hop time and within-segment variability (Q1 / viz 3)
    seg_hour = (hops.groupBy(*SEG_KEYS, "hour_of_day")
                .agg(F.count("*").alias("n"), F.avg("hop_sec").alias("m"), F.stddev_samp("hop_sec").alias("s"))
                .filter(F.col("n") >= th["min_trips_hourly"]).withColumn("cv", _cv(F.col("s"), F.col("m"))))
    var_h = seg_hour.groupBy("hour_of_day").agg(F.avg("cv").alias("mean_within_segment_cv"),
                                                F.count("*").alias("n_segment_hours"))
    base_h = hops.groupBy("hour_of_day").agg(F.count("*").alias("n_hops"),
                                             F.avg("hop_sec").alias("mean_hop_sec"),
                                             F.avg("hop_speed_kmh").alias("mean_speed_kmh"))
    save_table(base_h.join(var_h, "hour_of_day", "left"), "hourly_profile", tables_dir,
               order_by=["hour_of_day"])

    # ---- sample of hops for distribution plots (AM vs PM) -- seeded, documented
    hops.sample(False, 0.1, seed=7).select("hop_sec", "hop_speed_kmh", "time_bucket", "hour_of_day",
                                           "route_class").toPandas().to_parquet(
        Path(tables_dir) / "hop_sample_10pct.parquet", index=False)

    # ---- T12: named hotspots, matched against stop names that exist in the data
    stops_u = (visits.select("stop_id", "stop_name").distinct())
    rows = []
    stop_names = stops_u.toPandas()
    sp_pd_cols = ["from_stop_name", "to_stop_name", "PEAK_n", "OFF_PEAK_n", "PEAK_cv", "slowdown_ratio",
                  "PEAK_mean_speed_kmh", "PEAK_mean_hop_sec"]
    sp_pd = sp.select(*sp_pd_cols).toPandas()
    seg_pd = seg.select("from_stop_name", "to_stop_name", "PEAK_n", "OFF_PEAK_n", "PEAK_cv",
                        "slowdown_ratio", "PEAK_mean_speed_kmh").toPandas()
    matches, summary = [], []
    for pat in cfg["hotspot_patterns"]:
        m = stop_names[stop_names.stop_name.str.contains(pat, case=False, na=False, regex=False)]
        names = sorted(m.stop_name.unique())
        matches.append(dict(pattern=pat, n_stop_ids=len(m), n_distinct_names=len(names),
                            matched_names=" | ".join(names[:15]), status="matched" if len(m) else "no match in data"))
        if not len(m):
            continue
        for level, d in (("stop_pair", sp_pd), ("segment", seg_pd)):
            hit = d[d.from_stop_name.str.contains(pat, case=False, na=False, regex=False)
                    | d.to_stop_name.str.contains(pat, case=False, na=False, regex=False)]
            q = hit[(hit.PEAK_n >= minn) & (hit.OFF_PEAK_n >= minn)]
            summary.append(dict(pattern=pat, level=level, n_units_touching=len(hit), n_units_ranked=len(q),
                                median_peak_cv=q.PEAK_cv.median() if len(q) else None,
                                median_slowdown_ratio=q.slowdown_ratio.median() if len(q) else None,
                                median_peak_speed_kmh=q.PEAK_mean_speed_kmh.median() if len(q) else None))
    for nm, rws in (("hotspot_stop_matches", matches), ("hotspot_summary", summary)):
        pdf = pd.DataFrame(rws)
        pdf.to_csv(Path(tables_dir) / f"{nm}.csv", index=False)
        pdf.to_parquet(Path(tables_dir) / f"{nm}.parquet", index=False)
    classes.unpersist()
    seg.unpersist()
    sp.unpersist()


# --------------------------------------------------------------------------- CLI
def run_full(spark, cfg, service_day, tables_dir, interim_dir, explain_dir=None):
    wf = Waterfall()
    t0 = time.perf_counter()
    stage_clean(spark, cfg, f"{interim_dir}/stop_times_clean", wf)
    stage_join(spark, cfg, f"{interim_dir}/stop_times_clean", f"{interim_dir}/stop_visits",
               service_day, True, wf)
    stage_window_agg(spark, cfg, f"{interim_dir}/stop_visits", interim_dir, tables_dir, wf)
    wf.extras["pipeline_seconds_with_counting"] = round(time.perf_counter() - t0, 1)
    return wf


def main():
    ap = argparse.ArgumentParser(description="Run the full BMTC GTFS analysis pipeline.")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--service-day", choices=["weekday", "weekend"], default=None)
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--tables-dir", default=None)
    a = ap.parse_args()
    cfg = ingest.load_config(a.config)
    day = a.service_day or cfg["service_day"]
    tdir = a.tables_dir or cfg["paths"]["tables_dir"]
    spark = ingest.get_spark(a.cores, cfg["spark"]["shuffle_partitions"], cfg["spark"]["driver_memory"],
                             cfg["spark"]["adaptive_enabled"])
    wf = run_full(spark, cfg, day, tdir, cfg["paths"]["interim_dir"])
    wf.save(tdir)
    print(wf.to_pandas()[["stage", "rows_in", "rows_out", "pct_retained"]].to_string(index=False))
    print(json.dumps(wf.extras, indent=2))


if __name__ == "__main__":
    main()
