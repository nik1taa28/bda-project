"""All figures (PNG, 300 dpi).  (Nikitaa's area)

Every figure is about the Boston MBTA bus data (real actual vs scheduled times). Axes carry units;
Okabe-Ito colour-blind-safe palette; one y-axis per plot (no dual axes).
Run:  python -m src.viz
"""
import json
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src import ingest

BLUE, ORANGE, GREEN, VERMIL, SKY, PINK, YELLOW, GREY = ("#0072B2", "#E69F00", "#009E73", "#D55E00",
                                                         "#56B4E9", "#CC79A7", "#F0E442", "#7F7F7F")
INK, MUTED = "#222222", "#666666"
plt.rcParams.update({"font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "text.color": INK,
                     "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.color": "#E6E6E6",
                     "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.dpi": 100})
CAPTIONS = {}
BOS = "Boston MBTA bus, real actual vs scheduled times"


def save(fig, name, caption):
    fig.text(0.01, 0.005, textwrap.fill(caption, 150), fontsize=7.5, color=MUTED, va="bottom", ha="left")
    fig.tight_layout(rect=(0, 0.1 if len(caption) < 300 else 0.13, 1, 0.97))
    out = Path(OUT) / f"{name}.png"
    fig.savefig(out, dpi=300)
    plt.close(fig)
    CAPTIONS[name] = caption
    print("saved", out)


def fig_delay_overview(T, cfg):
    lo, hi = cfg["delay"]["on_time_early_sec"], cfg["delay"]["on_time_late_sec"]
    smp = pd.read_parquet(T / "delay_sample_2pct.parquet")
    hr = pd.read_csv(T / "delay_by_hour.csv")
    hw = pd.read_csv(T / "headway_by_hour.csv")
    ov = pd.read_csv(T / "delay_overall.csv").iloc[0]
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.6))
    x = smp.delay_sec.clip(-600, 1800) / 60
    ax[0].hist(x, bins=90, color=BLUE, alpha=.9)
    ax[0].axvspan(lo / 60, hi / 60, color=GREEN, alpha=.18, label=f"on time ({ov.on_time_rate:.0%})")
    ax[0].axvline(0, color=INK, lw=1)
    ax[0].set(xlabel="Delay = actual - scheduled departure (min), clipped to -10..30", ylabel="Events (2% sample)",
              title="Distribution of delay")
    ax[0].legend(frameon=False)
    ax[1].plot(hr.hour_of_day, hr.on_time_rate * 100, "o-", color=GREEN, lw=1.8, ms=4, label="on time")
    ax[1].plot(hr.hour_of_day, hr.late_rate * 100, "o-", color=VERMIL, lw=1.8, ms=4, label="late (> 5 min)")
    ax[1].set(xlabel="Scheduled hour of day", ylabel="Share of events (%)", title="On-time and late share by hour",
              xticks=range(0, 24, 3))
    ax[1].legend(frameon=False)
    ax[2].plot(hw.hour_of_day, hw.bunched_rate * 100, "o-", color=ORANGE, lw=1.8, ms=4, label="bunched (< 0.5x scheduled gap)")
    ax[2].plot(hw.hour_of_day, hw.gap_rate * 100, "o-", color=BLUE, lw=1.8, ms=4, label="large gap (> 1.5x)")
    ax[2].set(xlabel="Hour of day", ylabel="Share of high-frequency events (%)", title="Headway: bunching and gaps",
              xticks=range(0, 24, 3))
    ax[2].legend(frameon=False)
    fig.suptitle("Actual vs scheduled delay of buses at timepoints, " + BOS)
    save(fig, "fig1_delay_overview",
         f"{BOS}. {int(ov.n):,} departure events on 'Schedule'-standard trips (4 sampled months of 2024). On time = {lo//60} to +{hi//60} min of schedule. "
         f"Headway panel uses 'Headway'-standard trips: bunched = actual gap under half the scheduled gap.")


def fig_delay_routes(T, cfg):
    d = pd.read_csv(T / "delay_by_route.csv").head(12).iloc[::-1]
    pk = pd.read_csv(T / "delay_by_point_type.csv")
    tp = pd.read_csv(T / "delay_by_timepoint_order.csv")
    fig, ax = plt.subplots(1, 2, figsize=(13, 5))
    ax[0].barh(d.route_id.astype(str), d.mean_delay_sec / 60, color=VERMIL, height=.6)
    for y, (v, n) in enumerate(zip(d.mean_delay_sec / 60, d.n)):
        ax[0].text(v + .05, y, f"{v:.1f} min (n={int(n):,})", va="center", fontsize=7.5)
    ax[0].set(xlabel="Mean delay (min)", ylabel="Route", title="Most delayed routes (mean delay)")
    ax[0].set_xlim(0, (d.mean_delay_sec / 60).max() * 1.3)
    t = tp[tp.time_point_order <= 15]
    ax[1].plot(t.time_point_order, t.mean_delay_sec / 60, "o-", color=BLUE, lw=1.8, ms=5)
    ax[1].set(xlabel="Timepoint order along the trip (1 = start)", ylabel="Mean delay (min)",
              title="Mean delay by position in the trip")
    fig.suptitle("Where delay occurs, " + BOS)
    save(fig, "fig2_delay_routes_and_buildup",
         f"{BOS}. Routes ranked need at least {cfg['delay']['min_events']} events. Right: mean delay at the 1st, 2nd... timepoint of a trip "
         f"(first 15 timepoints); the curve is not monotonic and later timepoints cover far fewer trips.")


def fig_delay_slowdown(T, cfg):
    s = pd.read_parquet(T / "delay_segment_slowdown.parquet")
    dt = pd.read_csv(T / "delay_by_daytype_period.csv")
    fig, ax = plt.subplots(1, 2, figsize=(12, 5))
    ax[0].scatter(s.sched_slowdown_ratio, s.actual_slowdown_ratio, s=8, color=BLUE, alpha=.5, lw=0)
    ax[0].plot([.6, 2], [.6, 2], "--", color=GREY, lw=1.2, label="timetable matches reality")
    ax[0].set(xlabel="Scheduled slowdown ratio (peak / off-peak scheduled run time)",
              ylabel="Actual slowdown ratio (peak / off-peak actual run time)", xlim=(.6, 1.8), ylim=(.6, 1.8),
              title="Does the timetable plan the peak slowdown?")
    ax[0].legend(frameon=False)
    piv = dt.pivot(index="period", columns="day_type", values="mean_delay_sec") / 60
    piv.loc[["OFF_PEAK", "PEAK"]].plot.bar(ax=ax[1], color=[BLUE, ORANGE], rot=0, width=.6)
    ax[1].set(xlabel="Period (peak = 07-10h and 17-20h, scheduled hour)", ylabel="Mean delay (min)",
              title="Delay: peak vs off-peak, weekday vs weekend")
    ax[1].legend(title=None, frameon=False)
    fig.suptitle("Scheduled vs actual slowdown, " + BOS)
    save(fig, "fig3_actual_vs_scheduled_slowdown",
         f"{BOS}. Each point is a segment between consecutive timepoints of a route and direction with at least {cfg['delay']['min_events']} peak "
         f"and off-peak events ({len(s):,} segments). Points near the dashed line mean the timetable already plans the slowdown that really happens.")

def fig_waterfall(T):
    w = pd.read_csv(T / "delay_waterfall.csv")
    fig, ax = plt.subplots(figsize=(10, 5.2))
    col = {"start": BLUE, "quality": GREEN, "scope": ORANGE, "structural": SKY, "final": BLUE}
    names = [s.split(". ", 1)[1][:42] for s in w.stage]
    ax.bar(range(len(w)), w.rows_out / 1e6, color=[col[k] for k in w.kind], width=0.65)
    for i, (r, p) in enumerate(zip(w.rows_out, w.pct_retained)):
        ax.text(i, r / 1e6, f"{r/1e6:.2f}M\n{p:.1f}%", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(range(len(w))); ax.set_xticklabels(names, rotation=40, ha="right", fontsize=7.5)
    ax.set(ylabel="Rows remaining (millions)", title="Data-completeness waterfall (rows kept at each stage)")
    ax.set_ylim(0, w.rows_out.max() / 1e6 * 1.35)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=col[k], label=k) for k in ("quality", "scope")], frameon=False,
              title="Kind of step", loc="upper right", ncol=2)
    save(fig, "fig4_completeness_waterfall",
         f"{BOS}. Rows after each stage (percentage = share of the previous stage kept). 'quality' = defects (missing actual time, duplicates, "
         "implausible delay); 'scope' = Headway-standard trips are intentionally excluded from the delay analysis and the model.")


# ----------------------------------------------------------------------------- DELAY PREDICTION (ML)
def _ml(cfg):
    p = Path(cfg["paths"]["outputs_dir"]) / "ml_results.json"
    return json.loads(p.read_text()) if p.exists() else None


def fig_ml_regression(T, M):
    cal = pd.read_csv(T / "ml_regression_calibration.csv")
    hr = pd.read_csv(T / "ml_error_by_hour.csv")
    reg = M["regression"]
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.8))
    names = list(reg)
    short = ["train mean", "previous\ndelay carried", "GBT without\nprevious delay", "GBT\n(full)"]
    cols = [GREY, SKY, ORANGE, BLUE]
    mae = [reg[k]["mae_min"] for k in names]
    ax[0].bar(range(4), mae, color=cols, width=.6)
    for i, v in enumerate(mae):
        ax[0].text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax[0].set_xticks(range(4)); ax[0].set_xticklabels(short, fontsize=7.5)
    ax[0].set(ylabel="Mean absolute error on test days (min)", title="Error: model vs baselines (lower is better)")
    c = cal[cal.n >= 100]
    ax[1].plot([-3, 12], [-3, 12], "--", color=GREY, lw=1.2, label="perfect")
    ax[1].plot(c.mean_predicted_min, c.mean_actual_min, "o-", color=BLUE, lw=1.8, ms=5, label="GBT (full)")
    ax[1].set(xlabel="Predicted delay (min), binned", ylabel="Mean actual delay in the bin (min)", title="Calibration on test days")
    ax[1].legend(frameon=False)
    ax[2].plot(hr.hour_of_day, hr.mean_baseline_mae_min, "o-", color=GREY, lw=1.6, ms=4, label="train mean")
    ax[2].plot(hr.hour_of_day, hr.carry_baseline_mae_min, "o-", color=SKY, lw=1.6, ms=4, label="previous delay carried")
    ax[2].plot(hr.hour_of_day, hr.gbt_mae_min, "o-", color=BLUE, lw=2, ms=4, label="GBT (full)")
    ax[2].set(xlabel="Scheduled hour of day", ylabel="Mean absolute error (min)", title="Error by hour", xticks=range(0, 24, 3))
    ax[2].legend(frameon=False)
    fig.suptitle("Predicting delay in minutes (regression), " + BOS)
    r2 = reg["GBT (full features)"]["r2"]
    save(fig, "fig5_ml_regression",
         f"{BOS}. Trained on {M['train_rows_used']:,} events of the training days, evaluated on {M['test_rows']:,} events of held-out days "
         f"(last {M['test_fraction']:.0%} of each month). GBT test R2 = {r2:.2f}, RMSE = {reg['GBT (full features)']['rmse_min']:.2f} min.")


def fig_ml_classification(T, M):
    cal = pd.read_csv(T / "ml_classification_calibration.csv")
    clf = M["classification"]
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.8))
    names = list(clf)
    short = ["never late", "previous delay\ncarried", "GBT without\nprevious delay", "GBT (full)"]
    cols = [GREY, SKY, ORANGE, BLUE]
    metrics = ["precision", "recall", "f1"]
    w = .2
    for j, k in enumerate(names):
        vals = [clf[k][m] for m in metrics]
        ax[0].bar(np.arange(3) + (j - 1.5) * w, vals, width=w, color=cols[j], label=short[j].replace("\n", " "))
    ax[0].set_xticks(range(3)); ax[0].set_xticklabels(["precision", "recall", "F1"])
    ax[0].set(ylabel="Score on test days (late = positive)", title="Late / not-late classifier", ylim=(0, 1.05))
    ax[0].legend(frameon=False, fontsize=7)
    c = cal[cal.n >= 100]
    ax[1].plot([0, 1], [0, 1], "--", color=GREY, lw=1.2, label="perfectly calibrated")
    ax[1].plot(c.mean_predicted_prob, c.actual_late_rate, "o-", color=BLUE, lw=1.8, ms=5, label="GBT (full)")
    ax[1].set(xlabel="Predicted probability of being late", ylabel="Actual late share in the bin", title="Reliability on test days")
    ax[1].legend(frameon=False)
    f = clf["GBT (full features)"]
    cm = np.array([[f["tn"], f["fp"]], [f["fn"], f["tp"]]])
    ax[2].imshow(cm / cm.sum(1, keepdims=True), cmap="Blues", vmin=0, vmax=1)
    for i in range(2):
        for j in range(2):
            ax[2].text(j, i, f"{cm[i, j]:,}\n({cm[i, j] / cm[i].sum():.0%} of row)", ha="center", va="center",
                       color="white" if cm[i, j] / cm[i].sum() > .5 else INK, fontsize=8)
    ax[2].set_xticks([0, 1]); ax[2].set_xticklabels(["pred. on time", "pred. late"])
    ax[2].set_yticks([0, 1]); ax[2].set_yticklabels(["actually on time", "actually late"])
    ax[2].grid(False)
    ax[2].set_title("Confusion matrix (GBT, full)")
    fig.suptitle("Predicting 'will the bus be late (> 5 min)?' (classification), " + BOS)
    save(fig, "fig6_ml_classification",
         f"{BOS}. Held-out test days only ({M['test_rows']:,} events). Accuracy {f['accuracy']:.1%}, AUC {f['auc']:.3f}; threshold 0.5 on the "
         f"predicted probability. 'Never late' is the majority-class baseline.")


def fig_ml_importance(T, M):
    d = pd.read_csv(T / "ml_feature_importance.csv")
    d = d[d.model == "GBT (full features)"]
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.8))
    for a, task, col in zip(ax, ("regression", "classification"), (BLUE, VERMIL)):
        t = d[d.task == task].sort_values("importance")
        a.barh(t.feature, t.importance, color=col, height=.6)
        for y, v in enumerate(t.importance):
            a.text(v, y, f" {v:.2f}", va="center", fontsize=7.5)
        a.set(xlabel="Feature importance (share of total split gain)", title=f"{task.capitalize()} model")
        a.set_xlim(0, t.importance.max() * 1.2)
    fig.suptitle("What the gradient-boosted trees use to predict delay, " + BOS)
    save(fig, "fig7_ml_feature_importance",
         f"{BOS}. Importances of the full-feature GBT models (Spark MLlib featureImportances). A high importance shows the model relies on a "
         f"feature, not that it causes delay.")


# ----------------------------------------------------------------------------- benchmarks
def _speed_fig(sc, title, name, caption, lat_fmt):
    cores = sorted(int(c) for c in sc)
    sp = [sc[str(c)]["speedup_total"] for c in cores]
    lat = [sc[str(c)]["total_latency_s_median"] for c in cores]
    eff = [sc[str(c)]["efficiency_total"] for c in cores]
    fig, ax = plt.subplots(1, 3, figsize=(12, 4))
    ax[0].plot(cores, cores, "--", color=GREY, lw=1.5, label="ideal linear")
    ax[0].plot(cores, sp, "o-", color=BLUE, lw=2, ms=8, label="measured")
    for c, v in zip(cores, sp):
        ax[0].annotate(f"{v:.2f}", (c, v), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)
    ax[0].set(xlabel="Spark cores (local[N])", ylabel="Speedup S_N = T1 / TN (x)", xticks=cores, title="Speedup")
    ax[0].legend(frameon=False)
    ax[1].bar([str(c) for c in cores], lat, color=BLUE, width=.5)
    for i, v in enumerate(lat):
        ax[1].text(i, v, lat_fmt.format(v), ha="center", va="bottom", fontsize=8)
    ax[1].set(xlabel="Spark cores", ylabel="Median total latency (s)", title="Latency")
    ax[2].bar([str(c) for c in cores], eff, color=GREEN, width=.5)
    for i, v in enumerate(eff):
        ax[2].text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax[2].set(xlabel="Spark cores", ylabel="Efficiency S_N / N", title="Efficiency", ylim=(0, 1.1))
    fig.suptitle(title)
    save(fig, name, caption)


def fig_pipeline_speedup(B):
    env = B["environment"]
    _speed_fig(B["scaling"], "Delay analysis pipeline (ingest, clean, window, aggregate): speedup, latency, efficiency",
               "fig8_pipeline_speedup",
               f"{BOS}: {B['cleaned_event_rows']:,} cleaned events from months {B['config']['months']}. {B['config']['timed_runs']} "
               f"timed run(s) after {B['config']['warmup_runs']} discarded warm-up; each core count in its own process. Machine: {env['cpu']}, "
               f"{env['logical_cores']} logical cores ({env['performance_cores']} performance + {env['efficiency_cores']} efficiency), {env['ram_gb']} GB.",
               "{:.0f}")


def fig_model_speedup(B):
    m = B.get("model_scaling")
    if not m:
        return
    _speed_fig(m["scaling"], "Model training (features + GBT regressor + GBT classifier): speedup, latency, efficiency",
               "fig9_model_training_speedup",
               f"{BOS}: {m['train_rows_used']:,} training rows, {m['gbt']['gbt_max_iter']} boosting rounds, depth {m['gbt']['gbt_max_depth']}. "
               f"Median of {m['timed_runs']} timed run(s) after {m['warmup_runs']} discarded warm-up; each core count in its own process.",
               "{:.0f}")


def main():
    global OUT
    cfg = ingest.load_config()
    T, OUT = Path(cfg["paths"]["tables_dir"]), cfg["paths"]["figures_dir"]
    Path(OUT).mkdir(parents=True, exist_ok=True)
    if not (T / "delay_overall.csv").exists():
        raise SystemExit("delay tables not found: run python -m src.delay_pipeline first")
    fig_delay_overview(T, cfg); fig_delay_routes(T, cfg); fig_delay_slowdown(T, cfg); fig_waterfall(T)
    M = _ml(cfg)
    if M:
        fig_ml_regression(T, M); fig_ml_classification(T, M); fig_ml_importance(T, M)
    else:
        print("ml_results.json not found: skipping prediction figures")
    bp = Path(cfg["paths"]["outputs_dir"]) / "benchmark_results.json"
    if bp.exists():
        B = json.loads(bp.read_text())
        fig_pipeline_speedup(B); fig_model_speedup(B)
    else:
        print("benchmark_results.json not found: skipping speedup figures")
    Path(OUT, "captions.json").write_text(json.dumps(CAPTIONS, indent=2))


if __name__ == "__main__":
    main()
