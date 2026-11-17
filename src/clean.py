"""Cleaning rules + completeness waterfall (T3, T4, T5).  (Nikitaa's area)

Every step that can remove rows is recorded in the Waterfall: stage, rows in,
rows out, % retained and the reason. Counting is an extra Spark action, so the
benchmark switches it off (Waterfall(enabled=False)).
"""
import argparse
import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
from pyspark.sql import DataFrame, functions as F

from src import ingest
from src.gtfs_time import gtfs_time_to_sec


class Waterfall:
    """Collects (stage, rows_in, rows_out, reason, kind) in pipeline order.

    kind: 'quality'    = rows lost to bad/missing/orphan data
          'scope'      = rows intentionally excluded (day type)
          'structural' = rows that cannot form a hop (first stop of a trip)
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.rows: List[dict] = []
        self.extras: dict = {}
        self._prev: Optional[int] = None

    def start(self, stage: str, df: DataFrame, reason: str = "raw rows read"):
        if not self.enabled:
            return df
        n = df.count()
        self._prev = n
        self.rows.append(dict(stage=stage, rows_in=n, rows_out=n, pct_retained=100.0,
                              reason=reason, kind="start"))
        return df

    def step(self, stage: str, df: DataFrame, reason: str, kind: str = "quality"):
        if not self.enabled:
            return df
        n = df.count()
        rin = self._prev if self._prev is not None else n
        self.rows.append(dict(stage=stage, rows_in=rin, rows_out=n,
                              pct_retained=round(100.0 * n / rin, 4) if rin else 0.0,
                              reason=reason, kind=kind))
        self._prev = n
        return df

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    def save(self, tables_dir: str, name: str = "completeness_waterfall"):
        Path(tables_dir).mkdir(parents=True, exist_ok=True)
        self.to_pandas().to_csv(f"{tables_dir}/{name}.csv", index=False)
        Path(tables_dir, f"{name}_extras.json").write_text(json.dumps(self.extras, indent=2))


def clean_stop_times(st: DataFrame, wf: Optional[Waterfall] = None) -> DataFrame:
    """T1/T3/T4: key checks, dedupe, parse times to seconds, arrival/departure fallback."""
    wf = wf or Waterfall(enabled=False)
    wf.start("1. raw stop_times", st)

    st = st.filter(F.col("trip_id").isNotNull() & F.col("stop_id").isNotNull()
                   & F.col("stop_sequence").isNotNull())
    wf.step("2. drop null trip_id / stop_id / stop_sequence", st,
            "rows cannot be placed in a trip sequence")

    st = st.dropDuplicates(["trip_id", "stop_sequence"])          # T3
    wf.step("3. drop duplicate (trip_id, stop_sequence)", st, "duplicate stop visits")

    # T1: custom parser (handles 24:00:00+); T4: arrival/departure fallback
    arr = gtfs_time_to_sec(F.col("arrival_time"))
    dep = gtfs_time_to_sec(F.col("departure_time"))
    st = (st.withColumn("_arr", arr).withColumn("_dep", dep)
          .withColumn("arr_sec", F.coalesce("_arr", "_dep"))
          .withColumn("dep_sec", F.coalesce("_dep", "_arr")))
    if wf.enabled:
        r = st.agg(F.sum(F.when(F.col("_arr").isNull() & F.col("_dep").isNotNull(), 1).otherwise(0)).alias("arr_filled_from_dep"),
                   F.sum(F.when(F.col("_dep").isNull() & F.col("_arr").isNotNull(), 1).otherwise(0)).alias("dep_filled_from_arr"),
                   F.sum(F.when(F.col("arr_sec") >= 86400, 1).otherwise(0)).alias("rows_time_ge_24h")).first()
        wf.extras.update({k: int(r[k] or 0) for k in r.asDict()})
    st = st.drop("_arr", "_dep")
    st = st.filter(F.col("arr_sec").isNotNull())
    wf.step("4. drop rows with no usable arrival AND departure time", st,
            "both times blank/invalid (T4)")
    return st.select("trip_id", "stop_id", "stop_sequence", "arr_sec", "dep_sec",
                     "timepoint", "shape_dist_traveled")


def clean_dims(tables: dict):
    """Light cleaning of the dimension tables: keys present and unique."""
    trips = (tables["trips"].filter(F.col("trip_id").isNotNull() & F.col("route_id").isNotNull()
                                    & F.col("service_id").isNotNull())
             .dropDuplicates(["trip_id"])
             .select("trip_id", "route_id", "service_id",
                     F.coalesce("direction_id", F.lit(-1)).alias("direction_id")))
    routes = (tables["routes"].filter(F.col("route_id").isNotNull())
              .dropDuplicates(["route_id"])
              .select("route_id", "route_short_name", "route_long_name"))
    stops = (tables["stops"].filter(F.col("stop_id").isNotNull())
             .dropDuplicates(["stop_id"])
             .select("stop_id", "stop_name", "stop_lat", "stop_lon"))
    return trips, routes, stops


def main():
    ap = argparse.ArgumentParser(description="Run only the cleaning stage and print the waterfall.")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--cores", type=int, default=4)
    a = ap.parse_args()
    cfg = ingest.load_config(a.config)
    spark = ingest.get_spark(a.cores, cfg["spark"]["shuffle_partitions"], cfg["spark"]["driver_memory"])
    wf = Waterfall()
    st = ingest.read_gtfs(spark, cfg["paths"]["raw_dir"], "stop_times")
    clean_stop_times(st, wf)
    print(wf.to_pandas().to_string(index=False))
    print(wf.extras)


if __name__ == "__main__":
    main()
