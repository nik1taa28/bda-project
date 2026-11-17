"""Data profile for DATA_CARD.md (D1, D5, T13).  Run AFTER src.pipeline (reads its parquet).

Everything written to outputs/data_profile.json is measured here; DATA_CARD.md quotes it.
"""
import json
from pathlib import Path

from pyspark.sql import Window, functions as F

from src import ingest


def main():
    cfg = ingest.load_config()
    spark = ingest.get_spark(4, cfg["spark"]["shuffle_partitions"], cfg["spark"]["driver_memory"])
    raw = cfg["paths"]["raw_dir"]
    interim = cfg["paths"]["interim_dir"]
    prof = {"row_counts": {}}

    # ---- raw row counts per file (D1: use real numbers, never hard-code)
    tables = ingest.read_all(spark, raw)
    for n, df in tables.items():
        prof["row_counts"][n] = df.count()
    st = tables["stop_times"]
    r = st.agg(F.count("*").alias("n"),
               F.sum(F.when(F.col("arrival_time") == F.col("departure_time"), 1).otherwise(0)).alias("arr_eq_dep"),
               F.sum(F.when(F.col("timepoint") == 0, 1).otherwise(0)).alias("timepoint0"),
               F.sum(F.when(F.col("arrival_time").isNull() | F.col("departure_time").isNull(), 1).otherwise(0)).alias("blank_time"),
               F.sum(F.when(F.split("arrival_time", ":").getItem(0).cast("int") >= 24, 1).otherwise(0)).alias("hour_ge_24"),
               F.countDistinct("trip_id").alias("trips_with_stop_times"),
               F.countDistinct("stop_id").alias("stops_used")).first()
    prof["stop_times"] = {k: int(r[k]) for k in r.asDict()}
    prof["stop_times"]["pct_arrival_equals_departure"] = round(100 * r["arr_eq_dep"] / r["n"], 4)
    prof["stop_times"]["pct_timepoint_0"] = round(100 * r["timepoint0"] / r["n"], 4)

    # ---- route name patterns (T13): is the 'MF-' feeder prefix identifiable?
    routes = tables["routes"]
    prefix = F.regexp_extract("route_short_name", r"^([A-Za-z]+)-", 1)
    pc = (routes.withColumn("prefix", F.when(prefix != "", prefix)).groupBy("prefix").count()
          .orderBy(F.desc("count")).limit(12).toPandas())
    prof["route_name_prefix_counts"] = {str(k): int(v) for k, v in zip(pc.prefix.fillna("(none)"), pc["count"])}
    mf = routes.filter(F.col("route_short_name").startswith("MF-"))
    prof["mf_routes"] = mf.count()
    prof["mf_examples"] = [x[0] for x in mf.select("route_short_name").limit(6).collect()]
    prof["route_desc_counts"] = {str(k): int(v) for k, v in zip(*[routes.groupBy("route_desc").count()
                                 .orderBy(F.desc("count")).limit(6).toPandas()[c] for c in ("route_desc", "count")])}

    # ---- D5: how smooth / interpolated is the timetable?
    hops = spark.read.parquet(f"{interim}/hops")                # valid hops after all rules
    tmed = (hops.groupBy("trip_id").agg(F.percentile_approx("hop_speed_kmh", 0.5).alias("trip_med_speed"),
                                        F.count("*").alias("n")))
    j = hops.join(tmed, "trip_id")
    rel = F.abs(F.col("hop_speed_kmh") / F.col("trip_med_speed") - 1)
    s = j.agg(F.count("*").alias("n"),
              F.avg(F.when(rel <= 0.01, 1).otherwise(0)).alias("within_1pct"),
              F.avg(F.when(rel <= 0.05, 1).otherwise(0)).alias("within_5pct"),
              F.avg(F.when(rel <= 0.20, 1).otherwise(0)).alias("within_20pct"),
              F.avg("hop_speed_kmh").alias("mean_speed"), F.stddev_samp("hop_speed_kmh").alias("std_speed"),
              F.percentile_approx("hop_speed_kmh", [0.05, 0.25, 0.5, 0.75, 0.95]).alias("q")).first()
    prof["interpolation_profile"] = {
        "valid_hops": int(s["n"]),
        "pct_hops_within_1pct_of_trip_median_speed": round(100 * s["within_1pct"], 2),
        "pct_hops_within_5pct_of_trip_median_speed": round(100 * s["within_5pct"], 2),
        "pct_hops_within_20pct_of_trip_median_speed": round(100 * s["within_20pct"], 2),
        "mean_hop_speed_kmh": round(s["mean_speed"], 3), "std_hop_speed_kmh": round(s["std_speed"], 3),
        "speed_quantiles_p05_p25_p50_p75_p95": [round(x, 2) for x in s["q"]],
    }

    # ---- straight-line (haversine) vs feed shape_dist_traveled (units assumed metres; checked below)
    va = spark.read.parquet(f"{interim}/hops_all")
    w = Window.partitionBy("trip_id").orderBy("stop_sequence")
    d = (va.withColumn("shape_diff", F.col("shape_dist_traveled") - F.lag("shape_dist_traveled").over(w))
         .filter((F.col("shape_diff") > 50) & (F.col("shape_diff") < 20000) & F.col("hop_dist_m").isNotNull()
                 & (F.col("hop_sec") > 0))
         .withColumn("ratio", F.col("hop_dist_m") / F.col("shape_diff"))
         .withColumn("shape_speed", (F.col("shape_diff") / 1000) / (F.col("hop_sec") / 3600)))
    q = d.agg(F.count("*").alias("n"), F.percentile_approx("ratio", [0.1, 0.5, 0.9]).alias("q"),
              F.percentile_approx("shape_speed", [0.05, 0.25, 0.5, 0.75, 0.95]).alias("sq")).first()
    prof["haversine_vs_shape_dist"] = {
        "hops_compared": int(q["n"]),
        "haversine_over_shape_dist_p10_p50_p90": [round(x, 3) for x in q["q"]],
        "shape_dist_based_speed_kmh_p05_p25_p50_p75_p95": [round(x, 2) for x in q["sq"]],
        "note": "shape_dist_traveled has negative/large jumps in places, so it is only used for this check; "
                "pipeline distance is haversine (T10)."}
    Path("outputs").mkdir(exist_ok=True)
    Path("outputs/data_profile.json").write_text(json.dumps(prof, indent=2))
    print(json.dumps(prof, indent=2))


if __name__ == "__main__":
    main()
