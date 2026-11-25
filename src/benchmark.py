"""Benchmark driver + worker.  (Pranavi's area)

Every configuration runs in its OWN Python process (a Spark master cannot change in-process):
    python -m src.benchmark                      # driver: launches workers as subprocesses
    python -m src.benchmark --model-only         # only the ML-training benchmark (merged into the results file)
    python -m src.benchmark --worker scaling --cores 2 ...    # one worker (called by the driver)
Stages are timed only after an action (every stage ends with a Parquet write or a fit).
Protocol: 1 discarded warm-up + N timed runs, fixed Spark config, no cache() carried between runs.

Experiments (all on the real Boston MBTA data):
  scaling : ingest + clean, then window + aggregation, at 1 / 2 / 4 cores
  join    : broadcast vs shuffle (sort-merge) join of the train-only history tables, with the plan and the
            shuffle bytes read from the Spark event log
  model   : feature preparation + training of the GBT regressor and classifier at 1 / 2 / 4 cores
"""
import argparse
import contextlib
import io
import json
import shutil
import statistics as stats
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from src import delay_model, delay_pipeline, ingest
from src.environment import detect

TMP = Path("outputs/_tmp")
RESULTS = Path("outputs/benchmark_results.json")
SUMMARY = Path("outputs/tables/benchmark_summary.csv")


def _summ(xs):
    return {"median": round(stats.median(xs), 3), "min": round(min(xs), 3), "max": round(max(xs), 3),
            "runs": [round(x, 3) for x in xs]}


# ------------------------------------------------------------------ worker side
def parse_event_log(log_dir: Path) -> dict:
    """Sum shuffle bytes per job group (= per timed run) from the Spark event log."""
    files = [p for p in log_dir.iterdir() if p.is_file()] if log_dir.exists() else []
    if not files:
        return {}
    stage_group, per = {}, {}
    for line in max(files, key=lambda p: p.stat().st_size).read_text().splitlines():
        try:
            e = json.loads(line)
        except Exception:
            continue
        ev = e.get("Event")
        if ev == "SparkListenerJobStart":
            g = (e.get("Properties") or {}).get("spark.jobGroup.id")
            for sid in e.get("Stage IDs", []):
                stage_group[sid] = g
        elif ev == "SparkListenerTaskEnd":
            g = stage_group.get(e.get("Stage ID"))
            m = e.get("Task Metrics") or {}
            if g is None or not m:
                continue
            w = (m.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
            r = m.get("Shuffle Read Metrics") or {}
            rd = r.get("Remote Bytes Read", 0) + r.get("Local Bytes Read", 0)
            d = per.setdefault(g, {"shuffle_write_bytes": 0, "shuffle_read_bytes": 0})
            d["shuffle_write_bytes"] += w
            d["shuffle_read_bytes"] += rd
    return per


def worker(a):
    cfg = ingest.load_config()
    sp = cfg["spark"]
    work = TMP / f"{a.worker}_{a.variant}_{a.cores}c"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    extra, thr = {}, None
    evdir = work / "eventlog"
    if a.worker == "join":
        evdir.mkdir()
        extra = {"spark.eventLog.enabled": "true", "spark.eventLog.dir": f"file://{evdir.resolve()}"}
        thr = -1 if a.variant == "shuffle" else None      # -1 disables auto broadcast (sort-merge join)
    spark = ingest.get_spark(a.cores, sp["shuffle_partitions"], cfg["delay"]["bench_driver_memory"],
                             sp["adaptive_enabled"], broadcast_threshold=thr, extra=extra,
                             app=f"bench-{a.worker}-{a.cores}")
    tdir, clean_p = str(work / "tables"), str(work / "clean")
    rec = {"worker": a.worker, "variant": a.variant, "cores": a.cores,
           "shuffle_partitions": sp["shuffle_partitions"], "adaptive": sp["adaptive_enabled"],
           "auto_broadcast_threshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
           "warmup_runs": a.warmup, "timed_runs": a.runs, "months": cfg["delay"]["months"]}
    T = time.perf_counter

    if a.worker == "scaling":
        runs = []
        for i in range(a.warmup + a.runs):
            t0 = T(); delay_pipeline.stage_delay_clean(spark, cfg, clean_p); t1 = T()
            delay_pipeline.stage_delay_agg(spark, cfg, clean_p, tdir); t2 = T()
            r = {"clean": t1 - t0, "window_agg": t2 - t1, "total": t2 - t0}
            print(f"[{a.cores} cores] run {i} ({'warm-up, discarded' if i < a.warmup else 'timed'}):",
                  {k: round(v, 2) for k, v in r.items()}, flush=True)
            if i >= a.warmup:
                runs.append(r)
        rec["cleaned_rows"] = spark.read.parquet(clean_p).count()      # NOT timed
        rec["stages"] = {k: _summ([r[k] for r in runs]) for k in ("clean", "window_agg", "total")}

    elif a.worker == "join":
        interim = cfg["delay"]["interim_dir"]
        base = (delay_model.add_trip_features(spark.read.parquet(f"{interim}/events"), cfg)
                .withColumn("is_test", delay_model.test_flag(F.col("service_date"), cfg["delay_model"]["test_fraction"])))
        base.write.mode("overwrite").parquet(str(work / "base"))        # untimed setup
        base = spark.read.parquet(str(work / "base"))
        train_all, test_all = base.filter(~F.col("is_test")), base.filter(F.col("is_test"))
        use_b = a.variant != "shuffle"
        sc, runs = spark.sparkContext, []
        for i in range(a.warmup + a.runs):
            sc.setJobGroup(f"run{i}", f"join {a.variant} run {i}")
            out, _, _ = delay_model.add_history(train_all, test_all, use_b)
            if i == 0:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    out.explain()
                Path("outputs").mkdir(exist_ok=True)
                (Path("outputs") / f"explain_join_{a.variant}.txt").write_text(buf.getvalue().replace(str(Path.cwd()), "<project-root>"))
            t0 = T(); out.write.mode("overwrite").parquet(str(work / "joined")); dt = T() - t0
            print(f"[join {a.variant}] run {i}: {dt:.2f}s", flush=True)
            if i >= a.warmup:
                runs.append((i, dt))
        rec["join_seconds"] = _summ([d for _, d in runs])
        rec["_run_ids"] = [f"run{i}" for i, _ in runs]
        rec["rows_joined"] = spark.read.parquet(str(work / "joined")).count()
        spark.stop()
        per = parse_event_log(evdir)
        sw = [per[g]["shuffle_write_bytes"] for g in rec["_run_ids"] if g in per]
        sr = [per[g]["shuffle_read_bytes"] for g in rec["_run_ids"] if g in per]
        rec["shuffle_write_bytes_per_run_median"] = int(stats.median(sw)) if sw else "not measured"
        rec["shuffle_read_bytes_per_run_median"] = int(stats.median(sr)) if sr else "not measured"
        rec["plan_file"] = f"outputs/explain_join_{a.variant}.txt"

    elif a.worker == "model":
        runs = []
        for i in range(a.warmup + a.runs):
            t0 = T(); d = delay_model.prepare(spark, cfg, cfg["delay"]["interim_dir"]); t1 = T()
            mb = d["info"]["max_bins"]
            delay_model.fit_gbt("reg", d["train"], "features", cfg, mb, "bench_reg"); t2 = T()
            delay_model.fit_gbt("clf", d["train"], "features", cfg, mb, "bench_clf"); t3 = T()
            r = {"features": t1 - t0, "fit_regressor": t2 - t1, "fit_classifier": t3 - t2, "total": t3 - t0}
            print(f"[model {a.cores} cores] run {i} ({'warm-up, discarded' if i < a.warmup else 'timed'}):",
                  {k: round(v, 2) for k, v in r.items()}, flush=True)
            rec["train_rows_used"], rec["test_rows"] = d["info"]["train_rows_used"], d["info"]["test_rows"]
            for k in ("train", "test", "base"):
                d[k].unpersist()
            if i >= a.warmup:
                runs.append(r)
        rec["stages"] = {k: _summ([r[k] for r in runs]) for k in ("features", "fit_regressor", "fit_classifier", "total")}
        rec["gbt"] = {k: cfg["delay_model"][k] for k in ("gbt_max_iter", "gbt_max_depth", "max_train_rows")}
    Path(a.out).write_text(json.dumps(rec, indent=2))
    try:
        spark.stop()
    except Exception:
        pass
    shutil.rmtree(work, ignore_errors=True)


# ------------------------------------------------------------------ driver side
def launch(worker_name, cores, out, runs, warmup, variant="broadcast"):
    cmd = [sys.executable, "-m", "src.benchmark", "--worker", worker_name, "--cores", str(cores),
           "--runs", str(runs), "--warmup", str(warmup), "--variant", variant, "--out", str(out)]
    print(">>", " ".join(cmd), flush=True)
    log = Path(str(out) + ".log").open("w")
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=log, text=True)   # separate process
    print(p.stdout, end="")
    if p.returncode != 0:
        raise SystemExit(f"worker failed, see {out}.log")
    return json.loads(Path(out).read_text())


def _scaling(sc, cores, env, stage_keys, rows=None):
    base = sc[cores[0]]["stages"]
    out = {}
    for n, r in sc.items():
        s = r["stages"]
        e = {"oversubscribed": n > (env["logical_cores"] or 0), "stages_seconds": s,
             "total_latency_s_median": s["total"]["median"],
             "speedup_total": round(base["total"]["median"] / s["total"]["median"], 3),
             "efficiency_total": round(base["total"]["median"] / s["total"]["median"] / n, 3),
             "speedup_by_stage": {k: round(base[k]["median"] / s[k]["median"], 3) for k in stage_keys}}
        if rows:
            e["throughput_rows_per_s"] = round(rows / s["total"]["median"])
        out[str(n)] = e
    return out


def driver(a):
    cfg = ingest.load_config()
    bm, d, dm = cfg["benchmark"], cfg["delay"], cfg["delay_model"]
    TMP.mkdir(parents=True, exist_ok=True)
    env = detect(bm["cores"])
    cores = bm["cores"]
    res = json.loads(RESULTS.read_text()) if (a.model_only and RESULTS.exists()) else {}
    if not a.model_only:
        Path("outputs/environment.json").write_text(json.dumps(env, indent=2))
        runs, warm = a.runs or d["bench_timed_runs"], d["bench_warmup_runs"]
        res = {"environment": env,
               "dataset": "Boston MBTA bus arrival/departure times 2024 (real scheduled vs actual times)",
               "config": {"shuffle_partitions": cfg["spark"]["shuffle_partitions"], "adaptive": cfg["spark"]["adaptive_enabled"],
                          "driver_memory": d["bench_driver_memory"], "timed_runs": runs, "warmup_runs": warm,
                          "months": d["months"]},
               "baseline": "single-node Spark local[1]"}
        sc = {n: launch("scaling", n, TMP / f"scaling_{n}.json", runs, warm) for n in cores}
        rows = sc[cores[0]]["cleaned_rows"]
        res["cleaned_event_rows"] = rows
        res["scaling"] = _scaling(sc, cores, env, ("clean", "window_agg"), rows)
        jc = cores[-1]
        res["join_comparison"] = {"cores": jc, "what": "joining the train-only route / route+timepoint history tables onto the test events",
                                  "note": "broadcast = F.broadcast hint on the small tables; shuffle = no hint and "
                                          "autoBroadcastJoinThreshold=-1 (sort-merge join); AQE off",
                                  "variants": {v: launch("join", jc, TMP / f"join_{v}.json", runs, warm, v)
                                              for v in ("broadcast", "shuffle")}}
    if not a.skip_model:
        mr, mw = dm["bench_timed_runs"], dm["bench_warmup_runs"]
        ms = {n: launch("model", n, TMP / f"model_{n}.json", mr, mw) for n in cores}
        res["model_scaling"] = {"what": "feature preparation + training of the GBT regressor and classifier",
                                "timed_runs": mr, "warmup_runs": mw,
                                "train_rows_used": ms[cores[0]]["train_rows_used"], "test_rows": ms[cores[0]]["test_rows"],
                                "gbt": ms[cores[0]]["gbt"],
                                "scaling": _scaling(ms, cores, env, ("features", "fit_regressor", "fit_classifier"))}
    RESULTS.write_text(json.dumps(res, indent=2))

    rows_csv = []
    for n, e in res.get("scaling", {}).items():
        rows_csv.append({"experiment": "pipeline_scaling", "cores": int(n), "variant": "broadcast",
                         "latency_median_s": e["total_latency_s_median"], "min_s": e["stages_seconds"]["total"]["min"],
                         "max_s": e["stages_seconds"]["total"]["max"], "throughput_rows_per_s": e["throughput_rows_per_s"],
                         "speedup": e["speedup_total"], "efficiency": e["efficiency_total"],
                         "oversubscribed": e["oversubscribed"]})
    for v, r in res.get("join_comparison", {}).get("variants", {}).items():
        rows_csv.append({"experiment": "join_comparison", "cores": res["join_comparison"]["cores"], "variant": v,
                         "latency_median_s": r["join_seconds"]["median"], "min_s": r["join_seconds"]["min"],
                         "max_s": r["join_seconds"]["max"], "shuffle_write_bytes": r["shuffle_write_bytes_per_run_median"]})
    for n, e in res.get("model_scaling", {}).get("scaling", {}).items():
        rows_csv.append({"experiment": "model_training_scaling", "cores": int(n), "variant": "gbt reg+clf",
                         "latency_median_s": e["total_latency_s_median"], "min_s": e["stages_seconds"]["total"]["min"],
                         "max_s": e["stages_seconds"]["total"]["max"], "speedup": e["speedup_total"],
                         "efficiency": e["efficiency_total"], "oversubscribed": e["oversubscribed"]})
    Path("outputs/tables").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_csv).to_csv(SUMMARY, index=False)
    print(json.dumps({k: res[k] for k in ("scaling", "model_scaling") if k in res}, indent=2)[:3000])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker", choices=["scaling", "join", "model"])
    ap.add_argument("--cores", type=int, default=1)
    ap.add_argument("--runs", type=int, default=0)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--variant", default="broadcast")
    ap.add_argument("--out", default="")
    ap.add_argument("--model-only", action="store_true", help="benchmark only the ML training; merge into the results file")
    ap.add_argument("--skip-model", action="store_true", help="skip the ML training benchmark")
    a = ap.parse_args()
    if a.worker:
        worker(a)
    else:
        driver(a)


if __name__ == "__main__":
    main()
