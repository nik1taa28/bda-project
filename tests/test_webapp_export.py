"""The web dashboard's JSON must match the measured outputs it is exported from (no hand-typed numbers)."""
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "outputs" / "tables"
NEEDED = [TABLES / "delay_segment_slowdown.parquet", TABLES / "delay_sample_2pct.parquet", TABLES / "ml_prediction_sample.parquet",
          ROOT / "outputs" / "ml_results.json", ROOT / "outputs" / "benchmark_results.json"]


def _exporter():
    spec = importlib.util.spec_from_file_location("export_webapp_data", ROOT / "scripts" / "export_webapp_data.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    if not all(p.exists() for p in NEEDED):
        pytest.skip("run the pipelines and benchmarks first (Parquet tables and benchmark_results.json are needed)")
    out = tmp_path_factory.mktemp("webdata")
    _exporter().build(out)
    return out


def _load(path: Path):
    def bad(const):  # NaN / Infinity are not valid JSON and would break fetch().json()
        raise ValueError(f"invalid JSON constant {const} in {path.name}")
    return json.loads(path.read_text(), parse_constant=bad)


def test_json_files_are_valid(built):
    for name in ("meta", "boston", "model", "pipeline"):
        assert _load(built / f"{name}.json")


def test_boston_numbers_match_tables(built):
    b = _load(built / "boston.json")
    src = pd.read_csv(TABLES / "delay_overall.csv").iloc[0]
    assert b["overall"]["n"] == int(src.n)
    assert b["overall"]["mean_delay_sec"] == pytest.approx(src.mean_delay_sec, abs=1e-3)
    assert b["overall"]["on_time_rate"] == pytest.approx(src.on_time_rate, abs=1e-4)
    assert len(b["by_route"]) == len(pd.read_csv(TABLES / "delay_by_route.csv"))
    assert len(b["seg_slowdown"]) == len(pd.read_parquet(TABLES / "delay_segment_slowdown.parquet"))
    h = b["delay_hist_min"]
    assert sum(h["counts"]) == h["n"]  # every sampled event lands in a bin (values are clipped, not dropped)


def test_model_numbers_match_results_file(built):
    m = _load(built / "model.json")
    src = json.loads((ROOT / "outputs" / "ml_results.json").read_text())
    assert m["results"]["test_rows"] == src["test_rows"]
    assert m["results"]["regression"]["GBT (full features)"]["mae_min"] == src["regression"]["GBT (full features)"]["mae_min"]
    assert len(m["feature_importance"]) == len(pd.read_csv(TABLES / "ml_feature_importance.csv"))
    assert len(m["regression_calibration"]) == len(pd.read_csv(TABLES / "ml_regression_calibration.csv"))
    assert 0 < len(m["prediction_sample"]) <= 1500


def test_benchmark_numbers_match_results_file(built):
    p = _load(built / "pipeline.json")
    src = json.loads((ROOT / "outputs" / "benchmark_results.json").read_text())
    for cores, e in src["scaling"].items():
        assert p["benchmark"]["scaling"][cores]["speedup_total"] == e["speedup_total"]
    assert p["benchmark"]["cleaned_event_rows"] == src["cleaned_event_rows"]
    if "model_scaling" in src:
        assert p["benchmark"]["model_scaling"]["scaling"].keys() == src["model_scaling"]["scaling"].keys()
    jc = src["join_comparison"]["variants"]
    assert p["benchmark"]["join_comparison"]["variants"]["broadcast"]["join_seconds"]["median"] == jc["broadcast"]["join_seconds"]["median"]


def test_committed_json_is_not_stale(built):
    """webapp/public/data must be regenerated whenever outputs change (python scripts/export_webapp_data.py)."""
    committed = ROOT / "webapp" / "public" / "data"
    if not committed.exists():
        pytest.skip("no committed dashboard data")
    for name in ("boston", "model", "pipeline"):
        assert _load(committed / f"{name}.json") == _load(built / f"{name}.json"), f"{name}.json is out of date"
