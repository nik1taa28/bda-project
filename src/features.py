"""Feature engineering: per-hop duration / distance / speed, time buckets, route class.

T7  hop_sec  = arr_sec(stop i) - dep_sec(stop i-1)   (NOT arrival minus previous arrival)
    dwell_sec = dep_sec - arr_sec
T8  invalid hops are flagged and filtered, thresholds come from config.yaml
T10 distance is the straight-line (haversine) distance between consecutive stops,
    so it UNDER-estimates road distance; hop speeds are therefore lower bounds.
T13 route_class is a heuristic (feeder by name prefix, trunk by length).
"""
import math
from typing import Optional

from pyspark.sql import Column, DataFrame, Window, functions as F

from src.clean import Waterfall
from src.gtfs_time import hour_of_day

EARTH_R_M = 6371008.8


def haversine_m(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in metres using only Spark column expressions."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dphi, dlmb = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dphi / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dlmb / 2) ** 2
    # NB: F.least() ignores nulls (least(1.0, null) = 1.0), so null inputs must be handled
    # explicitly or the first stop of a trip would get a 20,015 km "hop".
    a_clamped = F.least(F.lit(1.0), a)  # guard against rounding just above 1
    return F.when(a.isNotNull(), 2 * EARTH_R_M * F.asin(F.sqrt(a_clamped)))


def haversine_m_py(lat1, lon1, lat2, lon2) -> float:
    """Pure-python twin used in tests."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R_M * math.asin(math.sqrt(min(1.0, a)))


def time_bucket_expr(hour_col: Column, cfg: dict) -> Column:
    """AM_PEAK / PM_PEAK / OFF_PEAK from the clock hour (windows in config.yaml)."""
    expr = F.lit("OFF_PEAK")
    for name, (start, end) in cfg["time_buckets"].items():
        expr = F.when((hour_col >= start) & (hour_col < end), F.lit(name)).otherwise(expr)
    return expr


def period_expr(bucket_col: Column) -> Column:
    return F.when(bucket_col.isin("AM_PEAK", "PM_PEAK"), "PEAK").otherwise("OFF_PEAK")


def add_hop_features(visits: DataFrame, cfg: dict) -> DataFrame:
    """One row per stop visit, enriched with the hop that ENDS at this stop.

    Window: partition by trip, order by stop_sequence, lag() = previous stop.
    The first stop of every trip has no previous stop, so its hop columns are null.
    """
    w = Window.partitionBy("trip_id").orderBy("stop_sequence")
    df = (visits
          .withColumn("from_stop_id", F.lag("stop_id").over(w))
          .withColumn("from_stop_name", F.lag("stop_name").over(w))
          .withColumn("from_lat", F.lag("stop_lat").over(w))
          .withColumn("from_lon", F.lag("stop_lon").over(w))
          .withColumn("prev_dep_sec", F.lag("dep_sec").over(w))
          .withColumnRenamed("stop_id", "to_stop_id")
          .withColumnRenamed("stop_name", "to_stop_name")
          .withColumnRenamed("stop_lat", "to_lat")
          .withColumnRenamed("stop_lon", "to_lon"))
    df = (df.withColumn("hop_sec", F.col("arr_sec") - F.col("prev_dep_sec"))              # T7
          .withColumn("dwell_sec", F.col("dep_sec") - F.col("arr_sec"))
          .withColumn("hop_dist_m", haversine_m(F.col("from_lat"), F.col("from_lon"), F.col("to_lat"), F.col("to_lon")))  # T10
          .withColumn("hop_speed_kmh",
                      F.when(F.col("hop_sec") > 0,
                             (F.col("hop_dist_m") / 1000.0) / (F.col("hop_sec") / 3600.0)))
          # time of day is the clock hour at which the hop STARTS (departure from previous stop)
          .withColumn("hour_of_day", hour_of_day(F.col("prev_dep_sec")))
          .withColumn("time_bucket", time_bucket_expr(F.col("hour_of_day"), cfg))
          .withColumn("period", period_expr(F.col("time_bucket"))))
    return df


def filter_hops(hops_all: DataFrame, cfg: dict, wf: Optional[Waterfall] = None) -> DataFrame:
    """T8: drop rows that are not a valid hop; each rule is counted in the waterfall."""
    wf = wf or Waterfall(enabled=False)
    th = cfg["thresholds"]
    h = hops_all.filter(F.col("from_stop_id").isNotNull())
    wf.step("10. hop level: drop first stop of each trip", h,
            "no previous stop, so no hop (structural, not a data defect)", kind="structural")
    h = h.filter(F.col("hop_sec") > 0)
    wf.step("11. drop hop_sec <= 0", h, "zero/negative scheduled hop time")
    h = h.filter(F.col("dwell_sec") >= 0)
    wf.step("12. drop dwell_sec < 0", h, "departure earlier than arrival")
    h = h.filter(F.col("hop_dist_m") >= th["min_hop_dist_m"])
    wf.step(f"13. drop hop distance < {th['min_hop_dist_m']} m", h,
            "stops (almost) co-located; speed unstable")
    h = h.filter(F.col("hop_speed_kmh") <= th["max_speed_kmh"])
    wf.step(f"14. drop speed > {th['max_speed_kmh']} km/h", h,
            "implausible scheduled speed for a city bus")
    return h


def trip_summary(hops_all: DataFrame, cfg: dict) -> DataFrame:
    """One row per trip: start time, end time, duration, stops and straight-line length."""
    t = (hops_all.groupBy("trip_id", "route_id", "route_short_name", "direction_id", "service_id")
         .agg(F.min("dep_sec").alias("start_sec"), F.max("arr_sec").alias("end_sec"),
              F.count("*").alias("n_stops"),
              (F.sum("hop_dist_m") / 1000.0).alias("trip_km")))
    t = (t.withColumn("trip_duration_min", (F.col("end_sec") - F.col("start_sec")) / 60.0)
         .withColumn("start_hour", hour_of_day(F.col("start_sec")))
         .withColumn("time_bucket", time_bucket_expr(F.col("start_hour"), cfg))
         .withColumn("period", period_expr(F.col("time_bucket")))
         .filter((F.col("n_stops") >= 2) & (F.col("trip_duration_min") > 0)))
    return t


def classify_routes(trips: DataFrame, cfg: dict) -> DataFrame:
    """T13 heuristic route class: feeder = name prefix (e.g. 'MF-'); trunk = non-feeder
    route whose median trip length >= trunk_min_km; everything else = other."""
    rc = cfg["route_class"]
    r = (trips.groupBy("route_id", "route_short_name")
         .agg(F.percentile_approx("trip_km", 0.5).alias("route_km"), F.count("*").alias("n_trips")))
    is_feeder = F.lit(False)
    for p in rc["feeder_prefixes"]:
        is_feeder = is_feeder | F.col("route_short_name").startswith(p)
    return r.withColumn("route_class",
                        F.when(is_feeder, "feeder")
                        .when(F.col("route_km") >= rc["trunk_min_km"], "trunk")
                        .otherwise("other"))
