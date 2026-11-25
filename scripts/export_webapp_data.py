"""Export the project's measured results as small JSON files for the web dashboard (webapp/public/data/).

Every number in the dashboard comes from these files, which come from outputs/ (CSV, Parquet, JSON written by the
pipelines, benchmarks and tests). Nothing is typed in by hand.
Run:  python scripts/export_webapp_data.py [--out webapp/public/data]
"""
import argparse
import datetime
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
OUT_TABLES = ROOT / "outputs" / "tables"


def clean(v):
    """JSON-safe scalar: NaN/inf -> None, numpy -> python, floats rounded."""
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (float, np.floating)):
        if math.isnan(v) or math.isinf(v):
            return None
        return round(float(v), 5)
    return v


def records(df: pd.DataFrame, cols=None) -> list:
    if cols:
        df = df[cols]
    return [{k: clean(v) for k, v in r.items()} for r in df.to_dict("records")]


def csv(name: str) -> pd.DataFrame:
    return pd.read_csv(OUT_TABLES / f"{name}.csv")


def pq(name: str) -> pd.DataFrame:
    return pd.read_parquet(OUT_TABLES / f"{name}.parquet")


def jload(rel: str):
    p = ROOT / rel
    return json.loads(p.read_text()) if p.exists() else None


def hist(values: np.ndarray, lo: float, hi: float, width: float) -> dict:
    edges = np.arange(lo, hi + width, width)
    counts, _ = np.histogram(np.clip(values, lo, hi - 1e-9), bins=edges)
    return {"lo": lo, "hi": hi, "width": width, "counts": counts.tolist(), "n": int(len(values))}


def build_boston(cfg) -> dict:
    d = cfg["delay"]
    smp = pd.read_parquet(OUT_TABLES / "delay_sample_2pct.parquet")
    delay_hist = hist(smp.delay_sec.to_numpy() / 60.0, -10, 30, 0.5)
    seg = pq("delay_segment_slowdown")
    seg = seg[["route_id", "direction_id", "from_tp", "time_point_id", "PEAK_n", "OFF_PEAK_n", "sched_slowdown_ratio", "actual_slowdown_ratio"]]
    return {
        "overall": records(csv("delay_overall"))[0],
        "hop_overall": records(csv("delay_hop_overall"))[0],
        "by_hour": records(csv("delay_by_hour")),
        "by_route": records(csv("delay_by_route")),
        "by_daytype_period": records(csv("delay_by_daytype_period")),
        "by_point_type": records(csv("delay_by_point_type")),
        "by_timepoint_order": records(csv("delay_by_timepoint_order")),
        "hop_by_hour": records(csv("delay_hop_by_hour")),
        "headway_overall": records(csv("headway_overall"))[0],
        "headway_by_hour": records(csv("headway_by_hour")),
        "headway_by_route": records(csv("headway_by_route")),
        "delay_hist_min": delay_hist,
        "seg_slowdown": records(seg),
        "waterfall": records(csv("delay_waterfall")),
        "settings": {k: d[k] for k in ("months", "on_time_early_sec", "on_time_late_sec", "max_abs_delay_sec", "min_events",
                                       "headway_bunch_ratio", "headway_gap_ratio")},
    }


def build_model(cfg):
    """Prediction (ML) results: metrics, calibration, importances, error by hour, a small prediction sample."""
    ml = jload("outputs/ml_results.json")
    if ml is None:
        return None
    smp = pq("ml_prediction_sample").sample(n=min(1500, len(pq("ml_prediction_sample"))), random_state=7)
    return {
        "results": ml,
        "regression_calibration": records(csv("ml_regression_calibration")),
        "classification_calibration": records(csv("ml_classification_calibration")),
        "error_by_hour": records(csv("ml_error_by_hour")),
        "feature_importance": records(csv("ml_feature_importance")),
        "prediction_sample": [[clean(r.delay_min), clean(r.pred_reg_full), clean(r.late), clean(r.p_full)] for r in smp.itertuples()],
        "settings": cfg["delay_model"],
    }


def build_pipeline(cfg) -> dict:
    return {
        "benchmark": jload("outputs/benchmark_results.json"),
        "environment": jload("outputs/environment.json"),
        "correctness_delay": jload("outputs/delay_correctness_check.json"),
        "waterfall": records(csv("delay_waterfall")),
        "time_buckets": cfg["time_buckets"], "delay_model": cfg["delay_model"],
    }


def build(out: Path) -> dict:
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text())
    out.mkdir(parents=True, exist_ok=True)
    dmeta = jload("data/raw_delay/delay_snapshot_meta.json") or {}
    parts = {"boston": build_boston(cfg), "model": build_model(cfg), "pipeline": build_pipeline(cfg)}
    parts = {k: v for k, v in parts.items() if v is not None}
    meta = {
        "exported_on": datetime.date.today().isoformat(),
        "boston": {"title": dmeta.get("title"), "licence": dmeta.get("licence_on_item"), "months": cfg["delay"]["months"]},
        "files": {k: f"{k}.json" for k in parts},
    }
    for name, payload in parts.items():
        (out / f"{name}.json").write_text(json.dumps(payload, separators=(",", ":")))
    (out / "meta.json").write_text(json.dumps(meta, indent=2))
    return {"meta": meta, **parts}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "webapp" / "public" / "data"))
    a = ap.parse_args()
    build(Path(a.out))
    for p in sorted(Path(a.out).glob("*.json")):
        print(f"{p.name}: {p.stat().st_size/1024:.0f} KB")


if __name__ == "__main__":
    main()
