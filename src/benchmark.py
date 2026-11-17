"""Benchmark driver + worker.  (Pranavi's area)

T15  each configuration runs in its OWN Python process (a Spark master cannot change in-process):
       python -m src.benchmark                     # driver: launches workers as subprocesses
       python -m src.benchmark --worker scaling --cores 2 ...   # one worker (called by the driver)
T16  stages are timed only after an action (every stage ends with a Parquet write)
T17  1 discarded warm-up + N timed runs, fixed Spark config, no cache() carried between runs
T19  broadcast vs shuffle join, with explain() plans and shuffle bytes from the Spark event log
T18  optional synthetic scale-up (replicated data) - labelled as NOT real data
"""
import argparse
import json
import shutil
import statistics as stats
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

from src import delay_pipeline, ingest, pipeline
from src.environment import detect

TMP = Path("outputs/_tmp")


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
    sp, bm = cfg["spark"], cfg["benchmark"]
    work = TMP / f"{a.worker}_{a.variant}_{a.cores}c_x{a.factor}"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    extra, thr = {}, None
    evdir = work / "eventlog"
    if a.worker == "join":
        evdir.mkdir()
        extra = {"spark.eventLog.enabled": "true", "spark.eventLog.dir": f"file://{evdir.resolve()}"}
        thr = -1 if a.variant == "shuffle" else None      # -1 disables auto broadcast (sort-merge join)
    mem = (bm["synthetic_driver_memory"] if a.worker == "synthetic"
           else cfg["delay"]["bench_driver_memory"] if a.worker == "delay" else sp["driver_memory"])
    spark = ingest.get_spark(a.cores, sp["shuffle_partitions"], mem, sp["adaptive_enabled"],
                             broadcast_threshold=thr, extra=extra, app=f"bench-{a.worker}-{a.cores}")
    tdir, day = str(work / "tables"), cfg["service_day"]
    clean_p, visits_p = str(work / "clean"), str(work / "visits")
    rec = {"worker": a.worker, "variant": a.variant, "cores": a.cores, "factor": a.factor,
           "shuffle_partitions": sp["shuffle_partitions"], "adaptive": sp["adaptive_enabled"],
           "auto_broadcast_threshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
           "warmup_runs": a.warmup, "timed_runs": a.runs}
    use_b = a.variant != "shuffle"
    T = time.perf_counter

    if a.worker == "scaling":
        runs = []
        for i in range(a.warmup + a.runs):
            t0 = T(); pipeline.stage_clean(spark, cfg, clean_p); t1 = T()
            pipeline.stage_join(spark, cfg, clean_p, visits_p, day, True); t2 = T()
            pipeline.stage_window_agg(spark, cfg, visits_p, str(work / "w"), tdir); t3 = T()
            r = {"clean": t1 - t0, "join": t2 - t1, "window_agg": t3 - t2, "total": t3 - t0}
            print(f"[{a.cores} cores] run {i} ({'warm-up, discarded' if i < a.warmup else 'timed'}):",
                  {k: round(v, 2) for k, v in r.items()}, flush=True)
            if i >= a.warmup:
                runs.append(r)
        rec["cleaned_rows"] = spark.read.parquet(visits_p).count()      # NOT timed
        rec["stages"] = {k: _summ([r[k] for r in runs]) for k in ("clean", "join", "window_agg", "total")}

    elif a.worker == "join":
        pipeline.stage_clean(spark, cfg, clean_p)                        # untimed setup
        sc, runs = spark.sparkContext, []
        for i in range(a.warmup + a.runs):
            sc.setJobGroup(f"run{i}", f"join {a.variant} run {i}")
            expl = str(Path("outputs") / f"explain_join_{a.variant}.txt") if i == 0 else None
            t0 = T(); pipeline.stage_join(spark, cfg, clean_p, visits_p, day, use_b, None, expl); dt = T() - t0
            print(f"[join {a.variant}] run {i}: {dt:.2f}s", flush=True)
            if i >= a.warmup:
                runs.append((i, dt))
        rec["join_seconds"] = _summ([d for _, d in runs])
        rec["_run_ids"] = [f"run{i}" for i, _ in runs]
        rec["cleaned_rows"] = spark.read.parquet(visits_p).count()
        spark.stop()
        per = parse_event_log(evdir)
        sw = [per[g]["shuffle_write_bytes"] for g in rec["_run_ids"] if g in per]
        sr = [per[g]["shuffle_read_bytes"] for g in rec["_run_ids"] if g in per]
        rec["shuffle_write_bytes_per_run_median"] = int(stats.median(sw)) if sw else "not measured"
        rec["shuffle_read_bytes_per_run_median"] = int(stats.median(sr)) if sr else "not measured"
        rec["plan_file"] = f"outputs/explain_join_{a.variant}.txt"

    elif a.worker == "delay":
        # REAL scheduled-vs-actual data (Boston MBTA, months in config). Stages: ingest+clean, window+aggregation.
        runs = []
        for i in range(a.warmup + a.runs):
            t0 = T(); delay_pipeline.stage_delay_clean(spark, cfg, clean_p); t1 = T()
            delay_pipeline.stage_delay_agg(spark, cfg, clean_p, tdir); t2 = T()
            r = {"clean": t1 - t0, "window_agg": t2 - t1, "total": t2 - t0}
            print(f"[delay {a.cores} cores] run {i} ({'warm-up, discarded' if i < a.warmup else 'timed'}):",
                  {k: round(v, 2) for k, v in r.items()}, flush=True)
            if i >= a.warmup:
                runs.append(r)
        rec["raw_rows"] = spark.read.parquet(clean_p).count()          # NOT timed (rows after cleaning)
        rec["months"] = cfg["delay"]["months"]
        rec["stages"] = {k: _summ([r[k] for r in runs]) for k in ("clean", "window_agg", "total")}

    elif a.worker == "synthetic":
        # SYNTHETIC REPLICATION, NOT REAL DATA: copy cleaned+joined rows `factor` times, giving every
        # copy its own trip_id and route_id so group sizes grow like a bigger network would.
        from pyspark.sql import functions as F
        base = spark.read.parquet(str(Path(cfg["paths"]["interim_dir"]) / "stop_visits"))
        k = spark.range(a.factor).withColumnRenamed("id", "rep")
        big = (base.crossJoin(F.broadcast(k))
               .withColumn("trip_id", F.concat_ws("_r", "trip_id", "rep"))
               .withColumn("route_id", F.concat_ws("_r", "route_id", "rep")).drop("rep"))
        big.write.mode("overwrite").parquet(visits_p)                     # untimed setup
        rec["synthetic_rows"] = spark.read.parquet(visits_p).count()
        runs = []
        for i in range(a.warmup + a.runs):
            t0 = T(); pipeline.stage_window_agg(spark, cfg, visits_p, str(work / "w"), tdir); dt = T() - t0
            print(f"[synthetic x{a.factor}, {a.cores} cores] run {i}: {dt:.2f}s", flush=True)
            if i >= a.warmup:
                runs.append(dt)
        rec["window_agg_seconds"] = _summ(runs)
        rec["label"] = "synthetic replication, not real data"
    Path(a.out).write_text(json.dumps(rec, indent=2))
    try:
        spark.stop()
    except Exception:
        pass
    shutil.rmtree(work, ignore_errors=True)


# ------------------------------------------------------------------ driver side
def launch(worker_name, cores, out, runs, warmup, variant="broadcast", factor=1):
    cmd = [sys.executable, "-m", "src.benchmark", "--worker", worker_name, "--cores", str(cores),
           "--runs", str(runs), "--warmup", str(warmup), "--variant", variant, "--factor", str(factor),
           "--out", str(out)]
    print(">>", " ".join(cmd), flush=True)
    log = Path(str(out) + ".log").open("w")
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=log, text=True)   # separate process (T15)
    print(p.stdout, end="")
    if p.returncode != 0:
        raise SystemExit(f"worker failed, see {out}.log")
    return json.loads(Path(out).read_text())


def delay_driver(cfg, env):
    """Real-delay benchmark: 1/2/4 cores, each in its own process; merged into benchmark_results.json."""
    bm, d = cfg["benchmark"], cfg["delay"]
    sc = {n: launch("delay", n, TMP / f"delay_{n}.json", d["bench_timed_runs"], d["bench_warmup_runs"])
          for n in bm["cores"]}
    base = sc[bm["cores"][0]]
    rows = base["raw_rows"]
    out = {"dataset": "Boston MBTA bus arrival/departure times 2024 (real scheduled-vs-actual data, NOT Bengaluru)",
           "months": d["months"], "cleaned_rows": rows, "timed_runs": d["bench_timed_runs"],
           "warmup_runs": d["bench_warmup_runs"], "driver_memory": d["bench_driver_memory"], "scaling": {}}
    for n, r in sc.items():
        s, t1 = r["stages"], base["stages"]
        out["scaling"][str(n)] = {
            "oversubscribed": n > (env["logical_cores"] or 0), "stages_seconds": s,
            "total_latency_s_median": s["total"]["median"],
            "throughput_rows_per_s": round(rows / s["total"]["median"]),
            "speedup_total": round(t1["total"]["median"] / s["total"]["median"], 3),
            "efficiency_total": round(t1["total"]["median"] / s["total"]["median"] / n, 3),
            "speedup_by_stage": {k: round(t1[k]["median"] / s[k]["median"], 3) for k in ("clean", "window_agg")}}
    return out


def driver(a):
    cfg = ingest.load_config()
    bm = cfg["benchmark"]
    TMP.mkdir(parents=True, exist_ok=True)
    if a.delay_only:
        env = detect(bm["cores"])
        res = json.loads(Path("outputs/benchmark_results.json").read_text())
        res["delay_benchmark"] = delay_driver(cfg, env)
        Path("outputs/benchmark_results.json").write_text(json.dumps(res, indent=2))
        rows_csv = [{"experiment": "delay_scaling_real_data", "cores": int(n), "variant": "broadcast",
                     "latency_median_s": e["total_latency_s_median"], "min_s": e["stages_seconds"]["total"]["min"],
                     "max_s": e["stages_seconds"]["total"]["max"], "throughput_rows_per_s": e["throughput_rows_per_s"],
                     "speedup": e["speedup_total"], "efficiency": e["efficiency_total"],
                     "oversubscribed": e["oversubscribed"]} for n, e in res["delay_benchmark"]["scaling"].items()]
        old = pd.read_csv("outputs/tables/benchmark_summary.csv")
        old = old[old.experiment != "delay_scaling_real_data"]
        pd.concat([old, pd.DataFrame(rows_csv)]).to_csv("outputs/tables/benchmark_summary.csv", index=False)
        print(json.dumps(res["delay_benchmark"]["scaling"], indent=2))
        return
    env = detect(bm["cores"])
    Path("outputs/environment.json").write_text(json.dumps(env, indent=2))
    runs, warm = a.runs or bm["timed_runs"], bm["warmup_runs"]
    res = {"environment": env, "config": {"shuffle_partitions": cfg["spark"]["shuffle_partitions"],
                                          "adaptive": cfg["spark"]["adaptive_enabled"],
                                          "driver_memory": cfg["spark"]["driver_memory"],
                                          "service_day": cfg["service_day"], "timed_runs": runs, "warmup_runs": warm},
           "baseline": "single-node Spark local[1]"}
    sc = {n: launch("scaling", n, TMP / f"scaling_{n}.json", runs, warm) for n in bm["cores"]}
    t1 = sc[bm["cores"][0]]["stages"]
    rows = sc[bm["cores"][0]]["cleaned_rows"]
    res["cleaned_stop_times_rows"] = rows
    res["scaling"] = {}
    for n, r in sc.items():
        s = r["stages"]
        entry = {"oversubscribed": n > (env["logical_cores"] or 0), "stages_seconds": s,
                 "total_latency_s_median": s["total"]["median"],
                 "throughput_rows_per_s": round(rows / s["total"]["median"]),
                 "speedup_total": round(t1["total"]["median"] / s["total"]["median"], 3),
                 "efficiency_total": round(t1["total"]["median"] / s["total"]["median"] / n, 3),
                 "speedup_by_stage": {k: round(t1[k]["median"] / s[k]["median"], 3) for k in ("clean", "join", "window_agg")}}
        res["scaling"][str(n)] = entry
    jc = bm["cores"][-1]
    res["join_comparison"] = {"cores": jc, "note": "broadcast = F.broadcast hint on dimension tables; "
                              "shuffle = no hint and autoBroadcastJoinThreshold=-1 (sort-merge); AQE off",
                              "variants": {v: launch("join", jc, TMP / f"join_{v}.json", runs, warm, v)
                                           for v in ("broadcast", "shuffle")}}
    if a.synthetic:
        res["synthetic_scaleup"] = {"label": "synthetic replication, not real data", "results": []}
        for f in bm["synthetic_factors"]:
            for n in bm["synthetic_cores"]:
                res["synthetic_scaleup"]["results"].append(
                    launch("synthetic", n, TMP / f"syn_{f}_{n}.json", bm["synthetic_timed_runs"],
                           bm["synthetic_warmup_runs"], factor=f))
        res["synthetic_scaleup"]["config"] = {"cores": bm["synthetic_cores"], "factors": bm["synthetic_factors"],
                                              "timed_runs": bm["synthetic_timed_runs"],
                                              "warmup_runs": bm["synthetic_warmup_runs"],
                                              "driver_memory": bm["synthetic_driver_memory"],
                                              "stage_timed": "window + aggregation only"}
    else:
        res["synthetic_scaleup"] = "not measured"
    if Path("data/interim_delay/events").exists() and Path(cfg["delay"]["raw_dir"]).exists() and a.with_delay:
        res["delay_benchmark"] = delay_driver(cfg, env)
    # other required metrics from files the pipeline/tests wrote
    wf = Path("outputs/tables/completeness_waterfall.csv")
    if wf.exists():
        w = pd.read_csv(wf)
        raw, final = int(w.iloc[0].rows_out), int(w.iloc[-1].rows_out)
        res["data_completeness"] = {"raw_stop_times_rows": raw, "final_analysed_hops": final,
                                    "ratio_final_over_raw": round(final / raw, 4)}
    cc = Path("outputs/correctness_check.json")
    res["aggregation_correctness"] = json.loads(cc.read_text()) if cc.exists() else "not measured"
    Path("outputs/benchmark_results.json").write_text(json.dumps(res, indent=2))

    rows_csv = []
    for n, e in res["scaling"].items():
        rows_csv.append({"experiment": "scaling_real_data", "cores": int(n), "variant": "broadcast",
                         "latency_median_s": e["total_latency_s_median"],
                         "min_s": e["stages_seconds"]["total"]["min"], "max_s": e["stages_seconds"]["total"]["max"],
                         "throughput_rows_per_s": e["throughput_rows_per_s"], "speedup": e["speedup_total"],
                         "efficiency": e["efficiency_total"], "oversubscribed": e["oversubscribed"]})
    for v, r in res["join_comparison"]["variants"].items():
        rows_csv.append({"experiment": "join_comparison", "cores": jc, "variant": v,
                         "latency_median_s": r["join_seconds"]["median"], "min_s": r["join_seconds"]["min"],
                         "max_s": r["join_seconds"]["max"],
                         "shuffle_write_bytes": r["shuffle_write_bytes_per_run_median"]})
    if a.synthetic:
        base = {}
        for r in res["synthetic_scaleup"]["results"]:
            m = r["window_agg_seconds"]["median"]
            base.setdefault(r["factor"], {})[r["cores"]] = (m, r["synthetic_rows"])
        for f, d in base.items():
            for n, (m, nrows) in d.items():
                rows_csv.append({"experiment": "synthetic_scaleup (synthetic replication, not real data)",
                                 "cores": n, "variant": f"x{f}", "latency_median_s": m,
                                 "throughput_rows_per_s": round(nrows / m),
                                 "speedup": round(d[min(d)][0] / m, 3), "efficiency": round(d[min(d)][0] / m / n, 3)})
    Path("outputs/tables").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_csv).to_csv("outputs/tables/benchmark_summary.csv", index=False)
    print(json.dumps(res["scaling"], indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker", choices=["scaling", "join", "synthetic", "delay"])
    ap.add_argument("--cores", type=int, default=1)
    ap.add_argument("--runs", type=int, default=0)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--variant", default="broadcast")
    ap.add_argument("--factor", type=int, default=1)
    ap.add_argument("--out", default="")
    ap.add_argument("--synthetic", action="store_true", help="also run the synthetic scale-up test")
    ap.add_argument("--with-delay", action="store_true", help="also benchmark the real-delay pipeline")
    ap.add_argument("--delay-only", action="store_true",
                    help="benchmark only the real-delay pipeline and merge into benchmark_results.json")
    a = ap.parse_args()
    if a.worker:
        worker(a)
    else:
        driver(a)


if __name__ == "__main__":
    main()
