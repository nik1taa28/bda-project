"""All figures (PNG, 300 dpi).  (Nikitaa's area)

Rules followed: every title/caption says "scheduled" (never "actual"); axes carry units;
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
import seaborn as sns

from src import ingest

BLUE, ORANGE, GREEN, VERMIL, SKY, PINK, YELLOW, GREY = ("#0072B2", "#E69F00", "#009E73", "#D55E00",
                                                         "#56B4E9", "#CC79A7", "#F0E442", "#7F7F7F")
INK, MUTED = "#222222", "#666666"
CLASS_COL = {"feeder": ORANGE, "trunk": BLUE, "other": GREY}
plt.rcParams.update({"font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "text.color": INK,
                     "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.color": "#E6E6E6",
                     "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.dpi": 100})
CAPTIONS = {}


def save(fig, name, caption):
    fig.text(0.01, 0.005, textwrap.fill(caption, 150), fontsize=7.5, color=MUTED, va="bottom", ha="left")
    fig.tight_layout(rect=(0, 0.1 if len(caption) < 300 else 0.13, 1, 0.97))
    out = Path(OUT) / f"{name}.png"
    fig.savefig(out, dpi=300)
    plt.close(fig)
    CAPTIONS[name] = caption
    print("saved", out)


def _route_label(r):
    return f"{r.route_short_name} (dir {int(r.direction_id)})"


def fig_top_routes(T, minn):
    d = pd.read_csv(T / "top_routes_peak_variability.csv").head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh([_route_label(r) for r in d.itertuples()], d.PEAK_cv, color=BLUE, height=0.6)
    for y, (v, n) in enumerate(zip(d.PEAK_cv, d.PEAK_n)):
        ax.text(v + 0.005, y, f"{v:.2f}  (n={int(n)} trips)", va="center", fontsize=7.5)
    ax.set_xlabel("Peak-hour scheduled trip-duration variability (coefficient of variation, std/mean)")
    ax.set_title("Top 10 routes by scheduled peak-hour trip-duration variability")
    ax.set_xlim(0, d.PEAK_cv.max() * 1.3)
    save(fig, "fig1a_top_routes_peak_variability",
         f"Scheduled (timetable-implied) trip durations of trips starting 07-10h or 17-20h, weekday service. Only routes with at least "
         f"{minn} peak trips are ranked. Variability across trips of one route also reflects different stop patterns, not only timing.")


def fig_top_segments(T, minn):
    d = pd.read_csv(T / "top_segments_peak_variability.csv").head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 5))
    lab = [f"{r.route_short_name}: {r.from_stop_name[:22]} > {r.to_stop_name[:22]}" for r in d.itertuples()]
    ax.barh(lab, d.PEAK_cv, color=VERMIL, height=0.6)
    for y, (v, n) in enumerate(zip(d.PEAK_cv, d.PEAK_n)):
        ax.text(v + 0.005, y, f"{v:.2f} (n={int(n)})", va="center", fontsize=7.5)
    ax.set_xlabel("Peak-hour scheduled hop-time variability (coefficient of variation, std/mean across trips)")
    ax.set_title("Top 10 segments by scheduled peak-hour hop-time variability")
    ax.set_xlim(0, d.PEAK_cv.max() * 1.25)
    save(fig, "fig1b_top_segments_peak_variability",
         f"Segment = (route, direction, from stop, to stop). Scheduled hop times of trips starting 07-10h or 17-20h. Only segments with at "
         f"least {minn} peak trips are ranked. Most segments have zero scheduled variability (identical timetable on every trip).")


def fig_speedup(B):
    sc = B["scaling"]
    cores = sorted(int(c) for c in sc)
    sp = [sc[str(c)]["speedup_total"] for c in cores]
    lat = [sc[str(c)]["total_latency_s_median"] for c in cores]
    eff = [sc[str(c)]["efficiency_total"] for c in cores]
    fig, ax = plt.subplots(1, 3, figsize=(12, 4))
    ax[0].plot(cores, cores, "--", color=GREY, lw=1.5, label="ideal linear")
    ax[0].plot(cores, sp, "o-", color=BLUE, lw=2, ms=8, label="measured")
    for c, s in zip(cores, sp):
        ax[0].annotate(f"{s:.2f}", (c, s), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)
    ax[0].set(xlabel="Spark cores (local[N])", ylabel="Speedup S_N = T1 / TN (x)", xticks=cores,
              title="Speedup")
    ax[0].legend(frameon=False)
    ax[1].bar([str(c) for c in cores], lat, color=BLUE, width=0.5)
    for i, v in enumerate(lat):
        ax[1].text(i, v, f"{v:.1f}", ha="center", va="bottom", fontsize=8)
    ax[1].set(xlabel="Spark cores", ylabel="Median total latency (s)", title="Latency")
    ax[2].bar([str(c) for c in cores], eff, color=GREEN, width=0.5)
    for i, v in enumerate(eff):
        ax[2].text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax[2].set(xlabel="Spark cores", ylabel="Efficiency S_N / N", title="Efficiency", ylim=(0, 1.1))
    fig.suptitle("Scheduled-timetable pipeline: speedup, latency and efficiency (real GTFS data)")
    env = B["environment"]
    save(fig, "fig2_speedup_latency_efficiency",
         f"Median of {B['config']['timed_runs']} timed runs after {B['config']['warmup_runs']} discarded warm-up, each core count in its own process. "
         f"Machine: {env['cpu']}, {env['logical_cores']} logical cores ({env['performance_cores']} performance + {env['efficiency_cores']} efficiency), "
         f"{env['ram_gb']} GB. Cleaned rows: {B['cleaned_stop_times_rows']:,}. Dataset is small, so Spark overhead limits speedup.")


def fig_distributions(T):
    h = pd.read_parquet(T / "hop_sample_10pct.parquet")
    tr = pd.read_parquet(T / "trips_summary.parquet")
    prof = pd.read_csv(T / "hourly_profile.csv")
    fig, ax = plt.subplots(2, 2, figsize=(11, 7.5))
    for b, c in (("AM_PEAK", BLUE), ("PM_PEAK", ORANGE)):
        x = h[(h.time_bucket == b) & (h.hop_sec < 400)].hop_sec
        sns.kdeplot(x, ax=ax[0, 0], color=c, lw=1.8, label=f"{b.replace('_', ' ')} (n={len(x):,})", cut=0)
        y = tr[(tr.time_bucket == b) & (tr.trip_duration_min < 200)].trip_duration_min
        sns.kdeplot(y, ax=ax[0, 1], color=c, lw=1.8, label=f"{b.replace('_', ' ')} (n={len(y):,})", cut=0)
    ax[0, 0].set(xlabel="Scheduled hop time (s), clipped at 400 s", ylabel="Density", title="Hop time: AM (07-10h) vs PM (17-20h)")
    ax[0, 1].set(xlabel="Scheduled trip duration (min), clipped at 200", ylabel="Density", title="Trip duration: AM vs PM start")
    for a in ax[0]:
        a.legend(frameon=False)
    ax[1, 0].plot(prof.hour_of_day, prof.mean_hop_sec, "o-", color=BLUE, lw=1.8, ms=5)
    ax[1, 0].set(xlabel="Hour of day (hop start)", ylabel="Mean scheduled hop time (s)", title="Hourly mean scheduled hop time")
    ax[1, 1].plot(prof.hour_of_day, prof.mean_within_segment_cv, "o-", color=VERMIL, lw=1.8, ms=5)
    ax[1, 1].set(xlabel="Hour of day (hop start)", ylabel="Mean within-segment CV of hop time",
                 title="Hourly scheduled variability (segments with enough trips)")
    for a in ax[1]:
        a.set_xticks(range(0, 24, 3))
    fig.suptitle("Scheduled hop-time and trip-duration distributions, and hourly profile (weekday)")
    save(fig, "fig3_distributions_hourly",
         "Top row: 10% seeded random sample of valid hops; all trips. Bottom row: all valid hops; within-segment CV uses segment-hours with at "
         "least 5 trips. Early-morning and late-night hours have few trips, so their values are noisy.")


def fig_feeder_trunk(T, minn):
    s = pd.read_parquet(T / "segment_summary.parquet")
    s = s[(s.PEAK_n >= minn) & s.route_class.isin(["feeder", "trunk"])]
    fig, ax = plt.subplots(1, 2, figsize=(10, 4.8))
    order = ["feeder", "trunk"]
    pal = {k: CLASS_COL[k] for k in order}
    counts = s.route_class.value_counts()
    labels = [f"{k}\n(n={counts.get(k, 0):,} segments)" for k in order]
    sns.violinplot(data=s, x="route_class", y="PEAK_mean_speed_kmh", order=order, palette=pal, ax=ax[0],
                   inner="quartile", cut=0, linewidth=1)
    ax[0].set(xlabel="Route class (heuristic)", ylabel="Peak mean scheduled hop speed (km/h, straight-line)",
              title="Scheduled hop speed")
    ax[0].set_xticks([0, 1]); ax[0].set_xticklabels(labels)
    sns.boxplot(data=s, x="route_class", y="PEAK_cv", order=order, palette=pal, ax=ax[1], fliersize=2, linewidth=1)
    ax[1].set(xlabel="Route class (heuristic)", ylabel="Peak scheduled hop-time CV (std/mean)",
              title="Scheduled variability")
    ax[1].set_xticks([0, 1]); ax[1].set_xticklabels(labels)
    fig.suptitle("Metro-feeder vs long trunk routes: scheduled speed and variability")
    save(fig, "fig4_feeder_vs_trunk",
         f"Segments with at least {minn} peak trips. Feeder = route_short_name starts with 'MF-'; trunk = non-feeder with median trip length at "
         f"or above the config threshold. This is a heuristic, not an official BMTC label. Speeds use straight-line distance, so they are lower bounds.")


def fig_slowdown(T, minn):
    d = pd.read_csv(T / "top_stop_pairs_slowdown.csv").head(12).iloc[::-1]
    sm = pd.read_csv(T / "hotspot_stop_matches.csv")
    fig, ax = plt.subplots(1, 2, figsize=(14, 5.2), gridspec_kw={"width_ratios": [1.2, 1]})
    ax[0].barh([f"{r.from_stop_name[:20]} > {r.to_stop_name[:20]}" for r in d.itertuples()], d.slowdown_ratio - 1,
               left=1, color=VERMIL, height=0.6)
    ax[0].axvline(1, color=INK, lw=1)
    for y, (v, n) in enumerate(zip(d.slowdown_ratio, d.n_routes)):
        ax[0].text(v + 0.003, y, f"{v:.2f} ({int(n)} routes)", va="center", fontsize=7.5)
    ax[0].set(xlabel="Scheduled slowdown ratio = peak mean hop time / off-peak mean hop time (x)",
              title="Top stop pairs by scheduled peak slowdown")
    ax[0].set_xlim(0.99, d.slowdown_ratio.max() * 1.08)
    ax[1].axis("off")
    ax[1].grid(False)
    tab = sm[["pattern", "n_stop_ids", "n_distinct_names", "status"]].copy()
    tab.columns = ["Hotspot pattern", "Stop IDs", "Distinct names", "Match"]
    t = ax[1].table(cellText=tab.values, colLabels=tab.columns, loc="center", cellLoc="left")
    t.auto_set_font_size(False); t.set_fontsize(8); t.scale(1, 1.35)
    ax[1].set_title("Named-hotspot matches in stop_name")
    fig.suptitle("Scheduled peak vs off-peak slowdown (a timetable proxy for planned congestion, not a measurement)")
    save(fig, "fig5_slowdown_and_hotspots",
         f"Stop pairs pooled across routes with at least {minn} trips in both peak and off-peak. A ratio near 1.0 means the timetable assumes the "
         f"same hop time at all hours. Hotspot table lists substring matches against stop names; only matched names are analysed.")


def fig_waterfall(T):
    w = pd.read_csv(T / "completeness_waterfall.csv")
    fig, ax = plt.subplots(figsize=(10, 5.2))
    col = {"start": BLUE, "quality": GREEN, "scope": ORANGE, "structural": SKY, "final": BLUE}
    names = [s.split(". ", 1)[1][:38] for s in w.stage]
    ax.bar(range(len(w)), w.rows_out / 1e6, color=[col[k] for k in w.kind], width=0.65)
    for i, (r, p) in enumerate(zip(w.rows_out, w.pct_retained)):
        ax.text(i, r / 1e6, f"{r/1e6:.2f}M\n{p:.1f}%", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(range(len(w))); ax.set_xticklabels(names, rotation=40, ha="right", fontsize=7.5)
    ax.set(ylabel="Rows remaining (millions)", title="Data-completeness waterfall (rows kept at each stage)")
    ax.set_ylim(0, w.rows_out.max() / 1e6 * 1.35)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=col[k], label=k) for k in ("quality", "scope", "structural")], frameon=False,
              title="Kind of step", loc="upper right", ncol=3)
    save(fig, "fig6_completeness_waterfall",
         "Rows after each stage (percentage = share of the previous stage kept). 'scope' = weekday filter (intended); 'structural' = first stop of "
         "each trip cannot form a hop; 'quality' = defects. Headline ratio is final analysed hops over raw stop_times rows.")


def fig_map(T, minn):
    d = pd.read_csv(T / "stop_slowdown.csv")
    d = d[d.n_pairs >= 1]
    fig, ax = plt.subplots(figsize=(7.5, 7))
    lo, hi = d.mean_slowdown_ratio.quantile([0.02, 0.98])
    sc = ax.scatter(d.stop_lon, d.stop_lat, c=d.mean_slowdown_ratio, s=4, cmap="viridis", vmin=lo, vmax=hi, lw=0)
    fig.colorbar(sc, ax=ax, label="Mean scheduled slowdown ratio (peak / off-peak)", shrink=0.8)
    ax.set(xlabel="Longitude (deg E)", ylabel="Latitude (deg N)", title="Stops coloured by scheduled peak slowdown")
    ax.set_aspect(1 / np.cos(np.radians(12.97)))
    save(fig, "fig7_stop_slowdown_map",
         f"Each point is a stop that starts at least one stop pair with {minn}+ peak and {minn}+ off-peak trips; colour is the mean scheduled slowdown "
         f"ratio of those pairs (colour range clipped to the 2nd-98th percentile). No base map. Scheduled proxy, not measured congestion.")


def fig_synthetic(B):
    s = B.get("synthetic_scaleup")
    if not isinstance(s, dict):
        return
    df = pd.DataFrame([{"factor": r["factor"], "cores": r["cores"], "t": r["window_agg_seconds"]["median"],
                        "rows": r["synthetic_rows"]} for r in s["results"]])
    fig, ax = plt.subplots(1, 2, figsize=(10, 4.2))
    for c, col in zip(sorted(df.cores.unique()), (VERMIL, BLUE, GREEN)):
        d = df[df.cores == c].sort_values("factor")
        ax[0].plot(d.rows / 1e6, d.t, "o-", color=col, label=f"{c} core(s)")
    ax[0].set(xlabel="Input rows (millions, synthetic)", ylabel="Window + aggregation time (s)", title="Latency vs volume")
    ax[0].legend(frameon=False)
    c0, c1 = df.cores.min(), df.cores.max()
    m = df[df.cores == c0].set_index("factor").t / df[df.cores == c1].set_index("factor").t
    ax[1].plot(m.index, m.values, "o-", color=BLUE, lw=2)
    ax[1].set(xlabel="Replication factor (x)", ylabel=f"Speedup {c1} cores vs {c0} core (x)", title="Speedup vs volume")
    fig.suptitle("SYNTHETIC REPLICATION, NOT REAL DATA: scale-up test")
    save(fig, "fig8_synthetic_scaleup",
         "SYNTHETIC replication, not real data: cleaned rows copied k times with new trip_id/route_id. One timed run per point; window + aggregation stage only; 8 GB driver heap.")


# ----------------------------------------------------------------------------- REAL DELAY (Boston MBTA)
BOS = "Boston MBTA bus, real actual vs scheduled times (not Bengaluru)"


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
    save(fig, "fig9_delay_overview",
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
    save(fig, "fig10_delay_routes_and_buildup",
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
    save(fig, "fig11_actual_vs_scheduled_slowdown",
         f"{BOS}. Each point is a segment between consecutive timepoints of a route and direction with at least {cfg['delay']['min_events']} peak "
         f"and off-peak events ({len(s):,} segments). Points near the dashed line mean the timetable already plans the slowdown that really happens.")


def fig_delay_speedup(B):
    d = B.get("delay_benchmark")
    if not d:
        return
    sc = d["scaling"]
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
        ax[1].text(i, v, f"{v:.0f}", ha="center", va="bottom", fontsize=8)
    ax[1].set(xlabel="Spark cores", ylabel="Median total latency (s)", title="Latency")
    ax[2].bar([str(c) for c in cores], eff, color=GREEN, width=.5)
    for i, v in enumerate(eff):
        ax[2].text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax[2].set(xlabel="Spark cores", ylabel="Efficiency S_N / N", title="Efficiency", ylim=(0, 1.1))
    fig.suptitle("Delay pipeline on REAL large data: speedup, latency, efficiency")
    save(fig, "fig12_delay_speedup",
         f"{BOS}: {d['cleaned_rows']:,} cleaned events from months {d['months']}. Median of {d['timed_runs']} timed runs after {d['warmup_runs']} "
         f"discarded warm-up; each core count in its own process. Real data, not synthetic replication.")


def main():
    global OUT
    cfg = ingest.load_config()
    T, OUT = Path(cfg["paths"]["tables_dir"]), cfg["paths"]["figures_dir"]
    Path(OUT).mkdir(parents=True, exist_ok=True)
    minn = cfg["thresholds"]["min_trips"]
    fig_top_routes(T, minn); fig_top_segments(T, minn); fig_distributions(T)
    fig_feeder_trunk(T, minn); fig_slowdown(T, minn); fig_waterfall(T); fig_map(T, minn)
    if (T / "delay_overall.csv").exists():
        fig_delay_overview(T, cfg); fig_delay_routes(T, cfg); fig_delay_slowdown(T, cfg)
    else:
        print("delay tables not found: skipping delay figures")
    bp = Path(cfg["paths"]["outputs_dir"]) / "benchmark_results.json"
    if bp.exists():
        B = json.loads(bp.read_text())
        fig_speedup(B); fig_synthetic(B); fig_delay_speedup(B)
    else:
        print("benchmark_results.json not found: skipping speedup figures")
    Path(OUT, "captions.json").write_text(json.dumps(CAPTIONS, indent=2))


if __name__ == "__main__":
    main()
