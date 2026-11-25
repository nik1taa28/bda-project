"""Delay PREDICTION on the Boston MBTA bus data (Spark MLlib).

Task: at the moment a bus leaves its previous timepoint, predict how late it will be at the next one.
  * regression      -> delay in minutes (actual - scheduled)
  * classification  -> will the bus be LATE (more than `delay.on_time_late_sec`, 5 min by default)?

Honesty rules built into the code:
  * Only 'Schedule'-standard trips are used (the same population the delay analysis judges).
  * TEMPORAL split: in every month the last `test_fraction` of the days are held out. The model never
    sees those days in training; all metrics are computed on them only.
  * Features are known before the event: clock hour, weekday, month, route, direction, point type,
    timepoint order, the delay at the PREVIOUS timepoint of the same trip (earlier stops only) and
    route / route+timepoint historical averages computed on the TRAINING days only (broadcast joined).
  * Every model is compared with simple baselines (train mean, 'previous delay carried forward',
    'never late') so the numbers mean something. A second model without the previous-delay feature
    shows how much of the skill comes from it.
CLI:  python -m src.delay_model [--cores 4]
"""
import argparse
import json
import time
from pathlib import Path

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, Window, functions as F

from src import ingest
from src.tables import save_table

CATEGORICAL = ["route_id", "direction_id", "point_type"]
NUM_SCHEDULE = ["hour_of_day", "day_of_week", "month", "time_point_order",
                "route_hist_mean_delay_min", "route_hist_late_rate", "rtp_hist_mean_delay_min"]
NUM_PREV = ["prev_delay_min", "has_prev"]
FEATURE_LABELS = {"route_id_idx": "route", "direction_id_idx": "direction", "point_type_idx": "point type",
                  "hour_of_day": "hour of day", "day_of_week": "day of week", "month": "month",
                  "time_point_order": "timepoint order", "route_hist_mean_delay_min": "route avg delay (train)",
                  "route_hist_late_rate": "route late rate (train)",
                  "rtp_hist_mean_delay_min": "route+timepoint avg delay (train)",
                  "prev_delay_min": "previous-timepoint delay", "has_prev": "has previous timepoint"}


# ------------------------------------------------------------------ pure Spark pieces (unit-tested)
def add_trip_features(ev: DataFrame, cfg: dict) -> DataFrame:
    """Schedule trips + clock features + the delay at the PREVIOUS timepoint of the same trip (lag window).

    The window only looks backwards (lag), so a row never sees its own or a later delay of its trip."""
    hi = cfg["delay"]["on_time_late_sec"]
    w = Window.partitionBy("service_date", "half_trip_id").orderBy("time_point_order")
    df = ev.filter(F.col("standard_type") == "Schedule")
    prev = F.lag("delay_sec").over(w) / 60.0
    return (df.withColumn("hour_of_day", (F.floor(F.col("sched_sec") / 3600) % 24).cast("int"))
              .withColumn("day_of_week", F.dayofweek("service_date"))
              .withColumn("month", F.month("service_date"))
              .withColumn("delay_min", F.col("delay_sec") / 60.0)
              .withColumn("late", (F.col("delay_sec") > hi).cast("double"))
              .withColumn("has_prev", prev.isNotNull().cast("int"))
              .withColumn("prev_delay_min", F.coalesce(prev, F.lit(0.0))))


def test_flag(date_col, test_fraction: float):
    """True for the last `test_fraction` of the days of each month (temporal hold-out)."""
    days = F.dayofmonth(F.last_day(date_col))
    return F.dayofmonth(date_col) > F.floor(days * (1.0 - test_fraction))


def add_history(train: DataFrame, df: DataFrame, use_broadcast: bool = True):
    """Attach route and route+timepoint average delay computed on TRAINING rows only.

    The small aggregated tables are broadcast-joined (set use_broadcast=False for the shuffle variant used in
    the join benchmark). Unseen keys fall back to the route average, then to the global training mean.
    Returns (frame, global train mean delay in minutes, global train late rate)."""
    b = F.broadcast if use_broadcast else (lambda x: x)
    g = train.agg(F.avg("delay_min").alias("m"), F.avg("late").alias("l")).first()
    r = train.groupBy("route_id").agg(F.avg("delay_min").alias("route_hist_mean_delay_min"),
                                      F.avg("late").alias("route_hist_late_rate"))
    rt = train.groupBy("route_id", "time_point_id").agg(F.avg("delay_min").alias("rtp_hist_mean_delay_min"))
    out = (df.join(b(r), "route_id", "left").join(b(rt), ["route_id", "time_point_id"], "left")
             .withColumn("route_hist_mean_delay_min", F.coalesce("route_hist_mean_delay_min", F.lit(g["m"])))
             .withColumn("route_hist_late_rate", F.coalesce("route_hist_late_rate", F.lit(g["l"])))
             .withColumn("rtp_hist_mean_delay_min",
                         F.coalesce("rtp_hist_mean_delay_min", F.col("route_hist_mean_delay_min"))))
    return out, float(g["m"]), float(g["l"])


# ------------------------------------------------------------------ metrics
def reg_metrics(df: DataFrame, pred: str, y: str = "delay_min") -> dict:
    r = df.agg(F.count("*").alias("n"), F.sqrt(F.avg((F.col(y) - F.col(pred)) ** 2)).alias("rmse"),
               F.avg(F.abs(F.col(y) - F.col(pred))).alias("mae"),
               F.sum((F.col(y) - F.col(pred)) ** 2).alias("sse"),
               F.sum((F.col(y) - F.lit(df.agg(F.avg(y)).first()[0])) ** 2).alias("sst")).first()
    return {"rmse_min": r["rmse"], "mae_min": r["mae"], "r2": 1.0 - r["sse"] / r["sst"]}


def clf_metrics(df: DataFrame, pred: str, score: str = None, y: str = "late") -> dict:
    c = lambda cond: F.sum(cond.cast("long"))
    r = df.agg(c((F.col(pred) == 1) & (F.col(y) == 1)).alias("tp"), c((F.col(pred) == 1) & (F.col(y) == 0)).alias("fp"),
               c((F.col(pred) == 0) & (F.col(y) == 1)).alias("fn"), c((F.col(pred) == 0) & (F.col(y) == 0)).alias("tn")).first()
    tp, fp, fn, tn = (int(r[k] or 0) for k in ("tp", "fp", "fn", "tn"))
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    out = {"accuracy": (tp + tn) / (tp + fp + fn + tn), "precision": prec, "recall": rec,
           "f1": 2 * prec * rec / (prec + rec) if prec + rec else 0.0,
           "tp": tp, "fp": fp, "fn": fn, "tn": tn, "auc": None}
    if score:
        out["auc"] = BinaryClassificationEvaluator(labelCol=y, rawPredictionCol=score,
                                                   metricName="areaUnderROC").evaluate(df)
    return out


# ------------------------------------------------------------------ data preparation
def prepare(spark, cfg: dict, interim_dir: str, use_broadcast: bool = True) -> dict:
    """Features, temporal split, train-only history, fitted indexers/assemblers. Returns cached train/test frames."""
    m = cfg["delay_model"]
    t0 = time.perf_counter()
    base = add_trip_features(spark.read.parquet(f"{interim_dir}/events"), cfg)
    base = base.withColumn("is_test", test_flag(F.col("service_date"), m["test_fraction"])).cache()
    train_all, test_all = base.filter(~F.col("is_test")), base.filter(F.col("is_test"))
    info = {}
    for name, d in (("train", train_all), ("test", test_all)):
        r = d.agg(F.count("*").alias("n"), F.min("service_date").alias("d0"), F.max("service_date").alias("d1"),
                  F.countDistinct("service_date").alias("days"), F.avg("late").alias("late_rate"),
                  F.avg("delay_min").alias("mean_delay_min")).first()
        info[name] = {"rows": r["n"], "first_date": str(r["d0"]), "last_date": str(r["d1"]), "days": r["days"],
                      "late_rate": r["late_rate"], "mean_delay_min": r["mean_delay_min"]}
    # history from ALL training rows, then train on a capped sample so local-mode GBT stays quick
    train_h, g_mean, g_late = add_history(train_all, train_all, use_broadcast)
    cap = m["max_train_rows"]
    frac = min(1.0, cap / info["train"]["rows"])
    train_h = train_h.sample(False, frac, seed=m["seed"]) if frac < 1.0 else train_h
    test_h, _, _ = add_history(train_all, test_all, use_broadcast)
    n_routes = train_all.select("route_id").distinct().count()
    cat_sizes = n_routes + 2                                    # +1 for 'keep' bucket of unseen routes
    stages = [StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep") for c in CATEGORICAL]
    idx = [c + "_idx" for c in CATEGORICAL]
    stages += [VectorAssembler(inputCols=idx + NUM_SCHEDULE + NUM_PREV, outputCol="features"),
               VectorAssembler(inputCols=idx + NUM_SCHEDULE, outputCol="features_sched")]
    keep = ["service_date", "hour_of_day", "delay_min", "late", "has_prev", "prev_delay_min"]
    prep = Pipeline(stages=stages).fit(train_h)
    train_f = prep.transform(train_h).select(*keep, "features", "features_sched").cache()
    test_f = prep.transform(test_h).select(*keep, "features", "features_sched").cache()
    info["train_rows_used"] = train_f.count()
    info["test_rows"] = test_f.count()
    info.update(train_cap=cap, n_routes=n_routes, global_train_mean_delay_min=g_mean, global_train_late_rate=g_late,
                max_bins=max(m["max_bins"], cat_sizes), test_fraction=m["test_fraction"],
                prepare_seconds=round(time.perf_counter() - t0, 1))
    return {"train": train_f, "test": test_f, "base": base, "info": info,
            "feature_names": idx + NUM_SCHEDULE + NUM_PREV, "feature_names_sched": idx + NUM_SCHEDULE}


def fit_gbt(kind: str, train: DataFrame, features_col: str, cfg: dict, max_bins: int, tag: str):
    m = cfg["delay_model"]
    common = dict(featuresCol=features_col, maxIter=m["gbt_max_iter"], maxDepth=m["gbt_max_depth"],
                  maxBins=max_bins, seed=m["seed"])
    if kind == "reg":
        return GBTRegressor(labelCol="delay_min", predictionCol=f"pred_{tag}", **common).fit(train)
    return GBTClassifier(labelCol="late", predictionCol=f"pred_{tag}", **common).fit(train)


def _p_late(raw_col: str):
    """GBT classifier raw prediction is [-margin, +margin]; Spark's own probability is 1 / (1 + exp(-2 * margin))."""
    return 1.0 / (1.0 + F.exp(-2.0 * vector_to_array(raw_col)[1]))


# ------------------------------------------------------------------ full run
def run(spark, cfg: dict, tables_dir: str, interim_dir: str, results_path: str) -> dict:
    t_all = time.perf_counter()
    d = prepare(spark, cfg, interim_dir)
    train, test, info, mb = d["train"], d["test"], d["info"], d["info"]["max_bins"]
    fits, names = {}, {}
    variants = [("full", "features", d["feature_names"]), ("noprev", "features_sched", d["feature_names_sched"])]
    scored = test
    for tag, fcol, fnames in variants:
        for kind in ("reg", "clf"):
            t0 = time.perf_counter()
            model = fit_gbt(kind, train, fcol, cfg, mb, f"{kind}_{tag}")
            fits[f"{kind}_{tag}"] = {"fit_seconds": round(time.perf_counter() - t0, 1)}
            names[f"{kind}_{tag}"] = (model, fnames)
            scored = model.transform(scored)
            if kind == "clf":                                 # this PySpark version cannot rename the default output columns
                scored = scored.withColumnRenamed("rawPrediction", f"raw_clf_{tag}").drop("probability")
    gm, gl = info["global_train_mean_delay_min"], info["global_train_late_rate"]
    hi_min = cfg["delay"]["on_time_late_sec"] / 60.0
    scored = (scored.withColumn("pred_mean", F.lit(gm))
              .withColumn("pred_carry", F.when(F.col("has_prev") == 1, F.col("prev_delay_min")).otherwise(F.lit(gm)))
              .withColumn("p_full", _p_late("raw_clf_full"))
              .withColumn("p_noprev", _p_late("raw_clf_noprev"))
              .withColumn("never_late", F.lit(0.0))
              .withColumn("carry_late", F.when(F.col("has_prev") == 1, (F.col("prev_delay_min") > hi_min).cast("double"))
                          .otherwise(F.lit(0.0)))
              .withColumn("carry_score", F.when(F.col("has_prev") == 1, F.col("prev_delay_min")).otherwise(F.lit(gm)))
              .select("service_date", "hour_of_day", "delay_min", "late", "has_prev", "prev_delay_min",
                      "pred_mean", "pred_carry", "pred_reg_full", "pred_reg_noprev", "p_full", "p_noprev",
                      "pred_clf_full", "pred_clf_noprev", "never_late", "carry_late", "carry_score",
                      "raw_clf_full", "raw_clf_noprev").cache())
    scored.count()

    reg = {"baseline: train mean": reg_metrics(scored, "pred_mean"),
           "baseline: previous delay carried forward": reg_metrics(scored, "pred_carry"),
           "GBT without previous delay": reg_metrics(scored, "pred_reg_noprev"),
           "GBT (full features)": reg_metrics(scored, "pred_reg_full")}
    clf = {"baseline: never late": clf_metrics(scored, "never_late"),
           "baseline: previous delay carried forward": clf_metrics(scored, "carry_late", "carry_score"),
           "GBT without previous delay": clf_metrics(scored, "pred_clf_noprev", "raw_clf_noprev"),
           "GBT (full features)": clf_metrics(scored, "pred_clf_full", "raw_clf_full")}

    # importances
    imp = []
    for tag, label in (("full", "GBT (full features)"), ("noprev", "GBT without previous delay")):
        for kind in ("reg", "clf"):
            model, fnames = names[f"{kind}_{tag}"]
            for n, v in zip(fnames, model.featureImportances.toArray()):
                imp.append({"model": label, "task": "regression" if kind == "reg" else "classification",
                            "feature": FEATURE_LABELS.get(n, n), "importance": float(v)})
    # calibration / error views (small tables)
    T = tables_dir
    reg_cal = (scored.withColumn("bin", F.floor(F.greatest(F.lit(-3.0), F.least(F.lit(12.0), F.col("pred_reg_full")))))
               .groupBy("bin").agg(F.count("*").alias("n"), F.avg("pred_reg_full").alias("mean_predicted_min"),
                                   F.avg("delay_min").alias("mean_actual_min")))
    save_table(reg_cal, "ml_regression_calibration", T, order_by=["bin"])
    clf_cal = (scored.withColumn("bin", F.least(F.lit(9), F.floor(F.col("p_full") * 10)))
               .groupBy("bin").agg(F.count("*").alias("n"), F.avg("p_full").alias("mean_predicted_prob"),
                                   F.avg("late").alias("actual_late_rate")))
    save_table(clf_cal, "ml_classification_calibration", T, order_by=["bin"])
    by_hour = scored.groupBy("hour_of_day").agg(
        F.count("*").alias("n"), F.avg(F.abs(F.col("delay_min") - F.col("pred_reg_full"))).alias("gbt_mae_min"),
        F.avg(F.abs(F.col("delay_min") - F.col("pred_mean"))).alias("mean_baseline_mae_min"),
        F.avg(F.abs(F.col("delay_min") - F.col("pred_carry"))).alias("carry_baseline_mae_min"))
    save_table(by_hour, "ml_error_by_hour", T, order_by=["hour_of_day"])
    scored.sample(False, min(1.0, 20000 / info["test_rows"]), seed=7) \
        .select("delay_min", "pred_reg_full", "late", "p_full").toPandas() \
        .to_parquet(Path(T) / "ml_prediction_sample.parquet", index=False)

    rows = []
    for k, v in reg.items():
        rows.append({"task": "regression (delay, minutes)", "model": k, **v})
    for k, v in clf.items():
        rows.append({"task": "classification (late > 5 min)", "model": k,
                     **{kk: vv for kk, vv in v.items() if kk not in ("tp", "fp", "fn", "tn")}})
    pd.DataFrame(rows).to_csv(Path(T) / "ml_metrics.csv", index=False)
    pd.DataFrame(imp).to_csv(Path(T) / "ml_feature_importance.csv", index=False)
    res = {"split": {k: info[k] for k in ("train", "test")}, "train_rows_used": info["train_rows_used"],
           "train_cap": info["train_cap"], "test_rows": info["test_rows"], "n_routes": info["n_routes"],
           "test_fraction": info["test_fraction"], "late_threshold_sec": cfg["delay"]["on_time_late_sec"],
           "gbt": {k: cfg["delay_model"][k] for k in ("gbt_max_iter", "gbt_max_depth", "seed")},
           "max_bins": mb, "regression": reg, "classification": clf, "fits": fits,
           "features_full": d["feature_names"], "features_without_previous_delay": d["feature_names_sched"],
           "prepare_seconds": info["prepare_seconds"], "total_seconds": round(time.perf_counter() - t_all, 1)}
    Path(results_path).write_text(json.dumps(res, indent=2))
    return res


def main():
    ap = argparse.ArgumentParser(description="Train and evaluate the delay prediction models (Boston MBTA)")
    ap.add_argument("--cores", type=int, default=4)
    a = ap.parse_args()
    cfg = ingest.load_config()
    sp = cfg["spark"]
    spark = ingest.get_spark(a.cores, sp["shuffle_partitions"], cfg["delay"]["bench_driver_memory"],
                             sp["adaptive_enabled"], app="delay-model")
    res = run(spark, cfg, cfg["paths"]["tables_dir"], cfg["delay"]["interim_dir"],
              str(Path(cfg["paths"]["outputs_dir"]) / "ml_results.json"))
    print(pd.read_csv(Path(cfg["paths"]["tables_dir"]) / "ml_metrics.csv").round(4).to_string(index=False))
    print("train rows used", res["train_rows_used"], "test rows", res["test_rows"], "seconds", res["total_seconds"])


if __name__ == "__main__":
    main()
