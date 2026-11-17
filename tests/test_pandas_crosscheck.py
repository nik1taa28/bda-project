"""T22: recompute hop times, speeds and segment mean/std in pandas for a sample of trips and
assert they match the Spark pipeline output. Writes outputs/correctness_check.json."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src import pipeline
from src.features import haversine_m_py

ROOT = Path(__file__).resolve().parent.parent


def _pandas_hops(visits: pd.DataFrame, cfg) -> pd.DataFrame:
    th = cfg["thresholds"]
    v = visits.sort_values(["trip_id", "stop_sequence"]).copy()
    g = v.groupby("trip_id")
    for c in ("stop_id", "stop_name", "stop_lat", "stop_lon", "dep_sec"):
        v["prev_" + c] = g[c].shift(1)
    v = v[v.prev_stop_id.notna()].copy()
    v["hop_sec"] = v.arr_sec - v.prev_dep_sec
    v["dist"] = [haversine_m_py(a, b, c, d) for a, b, c, d in
                 zip(v.prev_stop_lat, v.prev_stop_lon, v.stop_lat, v.stop_lon)]
    v = v[(v.hop_sec > 0) & ((v.dep_sec - v.arr_sec) >= 0) & (v.dist >= th["min_hop_dist_m"])]
    v["speed"] = (v.dist / 1000) / (v.hop_sec / 3600)
    v = v[v.speed <= th["max_speed_kmh"]].copy()
    hour = (np.floor(v.prev_dep_sec / 3600) % 24).astype(int)
    pk = cfg["time_buckets"]
    v["period"] = np.where(((hour >= pk["AM_PEAK"][0]) & (hour < pk["AM_PEAK"][1]))
                           | ((hour >= pk["PM_PEAK"][0]) & (hour < pk["PM_PEAK"][1])), "PEAK", "OFF_PEAK")
    return v


def test_spark_matches_pandas(spark, cfg):
    visits_p, hops_p = ROOT / "data/interim/stop_visits", ROOT / "data/interim/hops"
    if not visits_p.exists() or not hops_p.exists():
        pytest.skip("run `python -m src.pipeline` first")
    visits = pd.read_parquet(visits_p)
    # sample: ALL trips of 40 random routes, so segments have several trips each
    rng = np.random.default_rng(42)
    routes = rng.choice(visits.route_id.unique(), 40, replace=False)
    visits = visits[visits.route_id.isin(routes)]
    trips = set(visits.trip_id)
    result = {"sample_routes": 40, "sample_trips": len(trips), "sample_stop_visits": len(visits),
              "passed": False}
    try:
        pdh = _pandas_hops(visits, cfg)
        sph = pd.read_parquet(hops_p)
        sph = sph[sph.trip_id.isin(trips)]
        # 1) hop-level: same hops survive, same hop_sec and speed
        key = ["trip_id", "to_stop_id"]
        m = pdh.rename(columns={"stop_id": "to_stop_id"}).merge(sph, on=key, how="outer", indicator=True)
        assert (m["_merge"] == "both").all(), "different hop sets (Spark vs pandas)"
        assert (m.hop_sec_x == m.hop_sec_y).all()
        speed_err = (m.speed - m.hop_speed_kmh).abs().max()
        assert speed_err < 1e-6
        result.update(hops_compared=len(m), max_abs_speed_diff_kmh=float(speed_err))

        # 2) segment level: Spark aggregation vs pandas groupby (mean / std per period)
        sdf = spark.createDataFrame(sph)
        sdf = sdf.withColumn("hop_dist_m", sdf["hop_dist_m"])
        keys = ["route_id", "direction_id", "from_stop_id", "to_stop_id"]
        sp = pipeline._pivot_periods(sdf, keys).toPandas()
        pdh2 = pdh.rename(columns={"prev_stop_id": "from_stop_id", "stop_id": "to_stop_id"})
        agg = pdh2.groupby(keys + ["period"]).hop_sec.agg(["count", "mean", "std"]).reset_index()
        worst = 0.0
        checked = 0
        for _, r in agg.iterrows():
            row = sp[(sp.route_id == r.route_id) & (sp.direction_id == r.direction_id)
                     & (sp.from_stop_id == r.from_stop_id) & (sp.to_stop_id == r.to_stop_id)]
            assert len(row) == 1
            row = row.iloc[0]
            assert row[f"{r.period}_n"] == r["count"]
            worst = max(worst, abs(row[f"{r.period}_mean_hop_sec"] - r["mean"]))
            if r["count"] >= 2:
                worst = max(worst, abs(row[f"{r.period}_std_hop_sec"] - r["std"]))
            checked += 1
        assert worst < 1e-6
        result.update(segment_groups_compared=checked, max_abs_diff_mean_std_sec=float(worst), passed=True)
    finally:
        out = ROOT / "outputs" / "correctness_check.json"
        out.parent.mkdir(exist_ok=True)
        out.write_text(json.dumps(result, indent=2))
