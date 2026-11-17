"""Haversine (T10), cleaning rules (T3-T5), hop rules (T7, T8) on tiny hand-made data."""
import pytest
from pyspark.sql import functions as F

from src import features
from src.clean import Waterfall, clean_stop_times
from src.ingest import SCHEMAS


def test_haversine_one_degree_latitude(spark):
    df = spark.createDataFrame([(12.0, 77.0, 13.0, 77.0)], ["a", "b", "c", "d"])
    got = df.select(features.haversine_m(F.col("a"), F.col("b"), F.col("c"), F.col("d"))).first()[0]
    assert got == pytest.approx(111195, rel=1e-3)
    assert got == pytest.approx(features.haversine_m_py(12, 77, 13, 77), rel=1e-9)


def test_haversine_null_input_gives_null_not_half_the_earth(spark):
    """Regression: F.least(1.0, null) = 1.0 used to turn a null into a 20,015 km hop."""
    df = spark.createDataFrame([(None, None, 13.0, 77.0)], "a double, b double, c double, d double")
    assert df.select(features.haversine_m(F.col("a"), F.col("b"), F.col("c"), F.col("d"))).first()[0] is None


def _stop_times(spark, rows):
    cols = ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
            "shape_dist_traveled", "timepoint"]
    return spark.createDataFrame(rows, schema=SCHEMAS["stop_times"])


def test_clean_rules(spark):
    rows = [("T1", "08:00:00", "08:00:30", "A", 1, 0.0, 1),
            ("T1", "08:00:00", "08:00:30", "A", 1, 0.0, 1),     # duplicate (trip, seq)
            ("T1", None, "08:05:00", "B", 2, 1.0, 0),           # arrival missing -> use departure
            ("T1", "08:10:00", None, "C", 3, 2.0, 0),           # departure missing -> use arrival
            ("T1", None, None, "D", 4, 3.0, 0),                 # both missing -> dropped
            ("T1", "25:10:00", "25:10:00", "E", 5, 4.0, 1),     # after midnight must survive
            ("T1", "08:00:00", "08:00:00", None, 6, 5.0, 1),    # null stop_id -> dropped
            ("T1", "xx:yy", "xx:yy", "F", 7, 6.0, 1)]           # invalid text -> dropped
    wf = Waterfall()
    out = clean_stop_times(_stop_times(spark, rows), wf)
    got = {r["stop_id"]: (r["arr_sec"], r["dep_sec"]) for r in out.collect()}
    assert set(got) == {"A", "B", "C", "E"}
    assert got["A"] == (28800, 28830)
    assert got["B"] == (29100, 29100)
    assert got["C"] == (29400, 29400)
    assert got["E"] == (90600, 90600)
    stages = {r["stage"]: r for r in wf.rows}
    assert stages["1. raw stop_times"]["rows_out"] == 8
    assert wf.rows[-1]["rows_out"] == 4
    assert wf.extras["arr_filled_from_dep"] == 1 and wf.extras["dep_filled_from_arr"] == 1
    assert wf.extras["rows_time_ge_24h"] == 1


def test_hop_time_uses_previous_departure_and_rules(spark, cfg):
    # stops ~ 0.01 deg lat apart = ~1112 m. Dwell of 60 s at B.
    lat = lambda i: 12.90 + 0.01 * i
    base = dict(route_id="R", route_short_name="1", route_long_name="x", service_id="s", direction_id=0)
    rows = [  # trip_id, stop_id, stop_name, lat, lon, seq, arr, dep
        ("T", "A", "A", lat(0), 77.5, 1, 28800, 28800),
        ("T", "B", "B", lat(1), 77.5, 2, 28900, 28960),     # hop 100 s, dwell 60 s
        ("T", "C", "C", lat(2), 77.5, 3, 29060, 29060),     # hop = 29060-28960 = 100 (NOT 160)
        ("T", "D", "D", lat(3), 77.5, 4, 29060, 29060),     # hop 0 s  -> dropped
        ("T", "E", "E", lat(4), 77.5, 5, 29065, 29065),     # 5 s for 1112 m = 800 km/h -> dropped
    ]
    df = spark.createDataFrame(
        [(t, base["route_id"], base["route_short_name"], base["route_long_name"], base["service_id"],
          base["direction_id"], s, n, la, lo, q, a, d, 1, 0.0) for t, s, n, la, lo, q, a, d in rows],
        "trip_id string, route_id string, route_short_name string, route_long_name string, service_id string,"
        "direction_id int, stop_id string, stop_name string, stop_lat double, stop_lon double,"
        "stop_sequence int, arr_sec int, dep_sec int, timepoint int, shape_dist_traveled double")
    hops_all = features.add_hop_features(df, cfg)
    by_to = {r["to_stop_id"]: r for r in hops_all.collect()}
    assert by_to["B"]["hop_sec"] == 100 and by_to["B"]["dwell_sec"] == 60
    assert by_to["C"]["hop_sec"] == 100
    assert by_to["A"]["hop_dist_m"] is None
    assert by_to["B"]["time_bucket"] == "AM_PEAK"           # 08:00 start
    wf = Waterfall()
    wf.start("x", hops_all)
    valid = features.filter_hops(hops_all, cfg, wf).collect()
    assert sorted(r["to_stop_id"] for r in valid) == ["B", "C"]
    assert by_to["B"]["hop_speed_kmh"] == pytest.approx(1111.95 / 1000 / (100 / 3600), rel=1e-3)
