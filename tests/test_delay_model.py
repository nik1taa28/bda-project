"""Delay model tests on tiny hand-made data: no leakage in the lag feature, temporal split, history from
training rows only, metrics helpers."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

from src import delay_model as dm

COLS = "service_date date, route_id string, direction_id string, half_trip_id string, stop_id string," \
       "time_point_id string, time_point_order int, point_type string, standard_type string," \
       "sched_sec int, act_sec int, delay_sec int, scheduled_headway int, headway int"


def _ev(spark, rows):
    full = [(d, "R1", "Inbound", t, "s", tp, o, "Midpoint", st, sched, sched + dly, dly, None, None)
            for d, t, tp, o, st, sched, dly in rows]
    return spark.createDataFrame(full, COLS)


def test_prev_delay_uses_only_earlier_timepoints(spark, cfg):
    d = dt.date(2024, 1, 5)
    ev = _ev(spark, [(d, "T", "a", 1, "Schedule", 28800, 60),
                     (d, "T", "b", 2, "Schedule", 29400, 400),
                     (d, "T", "c", 3, "Schedule", 30000, 120),
                     (d, "T", "x", 1, "Headway", 28800, 999)])       # headway trip is excluded
    out = {r["time_point_order"]: r for r in dm.add_trip_features(ev, cfg).collect()}
    assert set(out) == {1, 2, 3}
    assert out[1]["has_prev"] == 0 and out[1]["prev_delay_min"] == 0.0           # first timepoint: nothing earlier
    assert out[2]["prev_delay_min"] == pytest.approx(1.0)                        # delay of timepoint 1, not its own
    assert out[3]["prev_delay_min"] == pytest.approx(400 / 60.0)                 # delay of timepoint 2 (not 3)
    assert out[2]["late"] == 1.0 and out[1]["late"] == 0.0 and out[3]["late"] == 0.0   # late = delay > 300 s
    assert out[2]["hour_of_day"] == 8 and out[2]["delay_min"] == pytest.approx(400 / 60.0)


def test_prev_delay_does_not_cross_trips_or_days(spark, cfg):
    d1, d2 = dt.date(2024, 1, 5), dt.date(2024, 1, 6)
    ev = _ev(spark, [(d1, "T1", "a", 1, "Schedule", 28800, 500), (d2, "T1", "a", 1, "Schedule", 28800, 10),
                     (d1, "T2", "a", 1, "Schedule", 28800, 30)])
    rows = dm.add_trip_features(ev, cfg).collect()
    assert all(r["has_prev"] == 0 for r in rows)                                 # each is its own trip-day


def test_temporal_split_has_no_date_overlap(spark):
    days = [dt.date(2024, m, d) for m in (1, 4) for d in range(1, 29)]
    df = spark.createDataFrame([(d,) for d in days], "service_date date")
    df = df.withColumn("t", dm.test_flag(F.col("service_date"), 0.25))
    tr = {r["service_date"] for r in df.filter(~F.col("t")).collect()}
    te = {r["service_date"] for r in df.filter(F.col("t")).collect()}
    assert tr and te and not (tr & te)
    for m in (1, 4):                                                             # test days are the END of each month
        assert max(x for x in tr if x.month == m) < min(x for x in te if x.month == m)


def test_history_comes_from_training_rows_only(spark, cfg):
    d = dt.date(2024, 1, 5)
    tr = spark.createDataFrame([(d, "R1", "a", 1.0, 0.0), (d, "R1", "a", 3.0, 1.0)],
                               "service_date date, route_id string, time_point_id string, delay_min double, late double")
    te = spark.createDataFrame([(d, "R1", "a", 100.0), (d, "R9", "zz", -50.0)],
                               "service_date date, route_id string, time_point_id string, delay_min double")
    out, gm, gl = dm.add_history(tr, te)
    rows = {r["route_id"]: r for r in out.collect()}
    assert rows["R1"]["route_hist_mean_delay_min"] == pytest.approx(2.0)         # test delay 100 is NOT in it
    assert rows["R1"]["route_hist_late_rate"] == pytest.approx(0.5)
    assert rows["R9"]["route_hist_mean_delay_min"] == pytest.approx(gm) == pytest.approx(2.0)   # unseen route -> global train mean
    assert rows["R9"]["rtp_hist_mean_delay_min"] == pytest.approx(gm)


def test_metric_helpers(spark):
    df = spark.createDataFrame([(1.0, 2.0, 1.0, 1.0), (3.0, 3.0, 0.0, 0.0), (5.0, 3.0, 1.0, 0.0), (7.0, 7.0, 0.0, 0.0)],
                               "y double, p double, late double, pred double")
    r = dm.reg_metrics(df.withColumnRenamed("y", "delay_min"), "p")
    assert r["mae_min"] == pytest.approx(0.75) and r["rmse_min"] == pytest.approx((5 / 4) ** 0.5)
    c = dm.clf_metrics(df, "pred")
    assert (c["tp"], c["fp"], c["fn"], c["tn"]) == (1, 0, 1, 2)
    assert c["precision"] == 1.0 and c["recall"] == 0.5 and c["accuracy"] == 0.75


def test_gbt_trains_and_predicts_on_mini_frame(spark, cfg):
    from pyspark.ml.feature import VectorAssembler
    rows = [(float(i % 10), float(i % 10 > 5)) for i in range(200)]
    df = spark.createDataFrame(rows, "x double, late double").withColumn("delay_min", F.col("x") * 2)
    df = VectorAssembler(inputCols=["x"], outputCol="features").transform(df)
    small = {**cfg, "delay_model": {**cfg["delay_model"], "gbt_max_iter": 5, "gbt_max_depth": 3}}
    clf = dm.fit_gbt("clf", df, "features", small, 32, "t")
    reg = dm.fit_gbt("reg", df, "features", small, 32, "t")
    assert clf.transform(df).filter(F.col("pred_t") == F.col("late")).count() == 200
    assert reg.transform(df).select(F.avg(F.abs(F.col("pred_t") - F.col("delay_min")))).first()[0] < 1.0
