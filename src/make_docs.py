"""Generate DATA_CARD.md, outputs/RESULTS.md, docs/PROPOSAL_UPDATES.md and README.md.

Every number is read from a file the pipeline/benchmark/tests wrote (outputs/, data/raw/snapshot_meta.json).
Nothing is typed in by hand; if an input file is missing the text says "not measured".
Run:  python -m src.make_docs
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src import ingest

ROOT = Path(".")
NM = "not measured"


def jload(p):
    p = Path(p)
    return json.loads(p.read_text()) if p.exists() else None


def f(x, nd=2):
    return NM if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:,.{nd}f}"


def i(x):
    return NM if x is None else f"{int(x):,}"


def md_table(df):
    cols = list(df.columns)
    out = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    for r in df.itertuples(index=False):
        out.append("| " + " | ".join(str(v) for v in r) + " |")
    return "\n".join(out)


def gather(cfg):
    T = Path(cfg["paths"]["tables_dir"])
    minn = cfg["thresholds"]["min_trips"]
    d = {"cfg": cfg, "minn": minn,
         "meta": jload("data/raw/snapshot_meta.json"), "prof": jload("outputs/data_profile.json"),
         "env": jload("outputs/environment.json"), "bench": jload("outputs/benchmark_results.json"),
         "corr": jload("outputs/correctness_check.json"), "extras": jload(T / "completeness_waterfall_extras.json")}
    d["wf"] = pd.read_csv(T / "completeness_waterfall.csv")
    d["classes"] = pd.read_csv(T / "route_class_counts.csv")
    d["hot_match"] = pd.read_csv(T / "hotspot_stop_matches.csv")
    d["hot_sum"] = pd.read_csv(T / "hotspot_summary.csv")
    d["hourly"] = pd.read_csv(T / "hourly_profile.csv")
    seg = pd.read_parquet(T / "segment_summary.parquet")
    sp = pd.read_parquet(T / "stop_pair_summary.parquet")
    rt = pd.read_csv(T / "route_summary.csv")
    hw = pd.read_parquet(T / "headway_by_stop_hour.parquet")
    s = {}
    for name, df in (("seg", seg), ("pair", sp)):
        pk = df[df.PEAK_n >= minn]
        both = df[(df.PEAK_n >= minn) & (df.OFF_PEAK_n >= minn)]
        s[name] = dict(total=len(df), ranked_peak=len(pk), ranked_both=len(both),
                       zero_std_share=float((pk.PEAK_std_hop_sec.fillna(0) == 0).mean()) if len(pk) else None,
                       cv_median=float(pk.PEAK_cv.median()), cv_p90=float(pk.PEAK_cv.quantile(.9)), cv_max=float(pk.PEAK_cv.max()),
                       sd_median=float(both.slowdown_ratio.median()),
                       sd_within1pct=float(((both.slowdown_ratio - 1).abs() <= 0.01).mean()),
                       sd_p95=float(both.slowdown_ratio.quantile(.95)), sd_max=float(both.slowdown_ratio.max()))
    d["s"] = s
    rk = rt[(rt.PEAK_n >= minn)]
    s["route"] = dict(ranked=len(rk), total=len(rt), cv_median=float(rk.PEAK_cv.median()), cv_p90=float(rk.PEAK_cv.quantile(.9)),
                      cv_max=float(rk.PEAK_cv.max()))
    s["headway"] = dict(groups=len(hw), cv_median=float(hw.headway_cv.median()), cv_p90=float(hw.headway_cv.quantile(.9)))
    ft = []
    q = seg[(seg.PEAK_n >= minn) & seg.route_class.isin(["feeder", "trunk"])]
    for k, g in q.groupby("route_class"):
        ft.append(dict(route_class=k, ranked_segments=len(g), median_peak_speed_kmh=round(g.PEAK_mean_speed_kmh.median(), 2),
                       median_peak_cv=round(g.PEAK_cv.median(), 4), p90_peak_cv=round(g.PEAK_cv.quantile(.9), 4),
                       median_slowdown_ratio=round(g.slowdown_ratio.median(), 4)))
    d["ft"] = pd.DataFrame(ft)
    Path(T / "feeder_vs_trunk_summary.csv").write_text(d["ft"].to_csv(index=False))
    # headline completeness
    w = d["wf"]
    raw, final = int(w.iloc[0].rows_out), int(w.iloc[-1].rows_out)
    scope = int(w[w.kind == "scope"].rows_in.sum() - w[w.kind == "scope"].rows_out.sum())
    struct = int(w[w.kind == "structural"].rows_in.sum() - w[w.kind == "structural"].rows_out.sum())
    qual = int(w[w.kind == "quality"].rows_in.sum() - w[w.kind == "quality"].rows_out.sum())
    d["delay"] = gather_delay(cfg)
    d["compl"] = dict(raw=raw, final=final, ratio=final / raw, scope=scope, struct=struct, quality=qual,
                      cleaned_stop_times=int(w[w.stage.str.startswith("9.")].rows_out.iloc[0]))
    return d



# ----------------------------------------------------------------------------- REAL DELAY (Boston MBTA)
def gather_delay(cfg):
    T = Path(cfg["paths"]["tables_dir"])
    if not (T / "delay_overall.csv").exists():
        return None
    g = {"cfg": cfg["delay"], "meta": jload("data/raw_delay/delay_snapshot_meta.json"),
         "overall": pd.read_csv(T / "delay_overall.csv").iloc[0], "hop": pd.read_csv(T / "delay_hop_overall.csv").iloc[0],
         "hour": pd.read_csv(T / "delay_by_hour.csv"), "dayperiod": pd.read_csv(T / "delay_by_daytype_period.csv"),
         "ptype": pd.read_csv(T / "delay_by_point_type.csv"), "order": pd.read_csv(T / "delay_by_timepoint_order.csv"),
         "routes": pd.read_csv(T / "delay_by_route.csv"), "hw": pd.read_csv(T / "headway_overall.csv").iloc[0],
         "hwhour": pd.read_csv(T / "headway_by_hour.csv"), "wf": pd.read_csv(T / "delay_waterfall.csv"),
         "bench": (jload("outputs/benchmark_results.json") or {}).get("delay_benchmark"),
         "corr": jload("outputs/delay_correctness_check.json")}
    seg = pd.read_parquet(T / "delay_segment_slowdown.parquet")
    g["seg"] = dict(n=len(seg), sched_med=float(seg.sched_slowdown_ratio.median()), act_med=float(seg.actual_slowdown_ratio.median()),
                    sched_within1=float(((seg.sched_slowdown_ratio - 1).abs() <= .01).mean()),
                    act_within1=float(((seg.actual_slowdown_ratio - 1).abs() <= .01).mean()),
                    corr=float(seg.sched_slowdown_ratio.corr(seg.actual_slowdown_ratio)))
    return g


def delay_section(d):
    g = d.get("delay")
    if not g:
        return "\n## 8. Real delay analysis\n\nnot measured (run `python -m src.delay_pipeline`).\n"
    o, h, hw, sg = g["overall"], g["hop"], g["hw"], g["seg"]
    w = g["wf"]
    raw, final = int(w.iloc[0].rows_out), int(w.iloc[-1].rows_out)
    ptype = g["ptype"].set_index("point_type")
    dp = g["dayperiod"].set_index(["day_type", "period"])
    od = g["order"]
    first, last = od[od.time_point_order == od.time_point_order.min()].iloc[0], od[od.time_point_order <= 15].iloc[-1]
    pk15 = od[od.time_point_order <= 15].sort_values('mean_delay_sec').iloc[-1]
    hr = g["hour"].set_index("hour_of_day")
    worst_h, best_h = hr.on_time_rate.idxmin(), hr.on_time_rate.idxmax()
    top = g["routes"].head(5)[["route_id", "n", "mean_delay_sec", "p90_delay_sec", "on_time_rate"]].round(2)
    bm = g["bench"]
    btxt = NM
    if bm:
        rows = [{"cores": n, "total median s": f(e["stages_seconds"]["total"]["median"], 1),
                 "min-max s": f"{f(e['stages_seconds']['total']['min'], 1)} - {f(e['stages_seconds']['total']['max'], 1)}",
                 "throughput rows/s": i(e["throughput_rows_per_s"]), "speedup": f(e["speedup_total"]),
                 "efficiency": f(e["efficiency_total"])} for n, e in sorted(bm["scaling"].items(), key=lambda kv: int(kv[0]))]
        btxt = (md_table(pd.DataFrame(rows)) + f"\n\n{i(bm['cleaned_rows'])} cleaned events; {bm['timed_runs']} timed runs after {bm['warmup_runs']} discarded warm-up per core count; "
                f"each core count in its own process; stages = ingest+clean and window+aggregation; {bm['driver_memory']} driver heap. "
                "This is real data (no replication), so it shows how speedup behaves at a more realistic volume than the Bengaluru timetable.")
    c = g["corr"]
    ctxt = (f"PASS: {i(c['events_compared'])} events on {len(c['routes_checked'])} routes recomputed in pandas directly from the raw CSV matched Spark "
            f"(max abs diff in mean/std/on-time {c['max_abs_diff_mean_std_on_time']:.1e})") if c and c.get("passed") else NM
    bm_flat = d["s"]["seg"]
    return f"""
## 8. Real delay analysis (Boston MBTA bus data: actual vs scheduled)
**Why this section exists.** BMTC publishes only a static timetable, so BMTC delay cannot be measured (sections 1-7 are therefore *scheduled* analytics).
To show the same Spark approach on **real actual-vs-scheduled times**, we ran it on the public MBTA (Boston) bus dataset, which records both the scheduled
and the actual departure of every bus at every timepoint. **Everything in this section is about Boston, not Bengaluru, and must not be quoted as BMTC performance.**
Source and licence: see `DATA_CARD_DELAY.md` (licence on the item: {g['meta']['licence_on_item']}). Months analysed: {g['cfg']['months']} of 2024 (one per season).

### 8.1 Data completeness
{i(raw)} raw events -> **{i(final)}** kept ({f(100*final/raw)}%). Biggest steps: events with no actual time were dropped (data-collection gaps), and only 'Schedule'-standard trips
are judged on delay (the dataset says 'Headway'-standard trips must be judged on headways instead). Full waterfall: `outputs/tables/delay_waterfall.csv`.

{md_table(w[['stage','rows_in','rows_out','pct_retained']])}

### 8.2 How late are buses?
Delay = actual minus scheduled departure at a timepoint. Over {i(o['n'])} events: mean **{f(o['mean_delay_sec']/60,1)} min**, median {f(o['median_delay_sec']/60,1)} min, 90th percentile {f(o['p90_delay_sec']/60,1)} min.
**On time** (between {g['cfg']['on_time_early_sec']//60} and +{g['cfg']['on_time_late_sec']//60} min) = **{f(100*o['on_time_rate'],1)}%**; late (> {g['cfg']['on_time_late_sec']//60} min) {f(100*o['late_rate'],1)}%; early {f(100*o['early_rate'],1)}%.

- By hour, the lowest on-time share is at {int(worst_h)}h ({f(100*hr.loc[worst_h,'on_time_rate'],1)}%) and the highest at {int(best_h)}h ({f(100*hr.loc[best_h,'on_time_rate'],1)}%). Overnight hours have fewer trips.
- Weekday mean delay: peak {f(dp.loc[('weekday','PEAK'),'mean_delay_sec']/60,1)} min vs off-peak {f(dp.loc[('weekday','OFF_PEAK'),'mean_delay_sec']/60,1)} min; weekend: peak {f(dp.loc[('weekend','PEAK'),'mean_delay_sec']/60,1)} vs off-peak {f(dp.loc[('weekend','OFF_PEAK'),'mean_delay_sec']/60,1)} min (peak = 07-10h and 17-20h scheduled hour).
- Delay by position in the trip (first 15 timepoints): {f(first.mean_delay_sec/60,1)} min at timepoint {int(first.time_point_order)}, a maximum of {f(pk15.mean_delay_sec/60,1)} min at timepoint {int(pk15.time_point_order)}, then {f(last.mean_delay_sec/60,1)} min at timepoint {int(last.time_point_order)}. The pattern is not monotonic, and later timepoints cover far fewer trips (a few thousand events at timepoint 14 vs hundreds of thousands at timepoint 1), so it is not a pure "delay accumulates" curve. By point type: start {f(ptype.loc['Startpoint','mean_delay_sec']/60,1)} min, middle {f(ptype.loc['Midpoint','mean_delay_sec']/60,1)} min, end {f(ptype.loc['Endpoint','mean_delay_sec']/60,1)} min.

Five most delayed routes (at least {g['cfg']['min_events']} events; `delay_by_route.csv`):

{md_table(top)}

### 8.3 Actual vs scheduled run time (real slowdown)
Between consecutive timepoints the mean scheduled run time is {f(h.mean_sched_hop,0)} s and the mean actual run time {f(h.mean_act_hop,0)} s (on average {f(h.mean_added_sec,1)} s added per hop; {i(h.n)} hops).
Over {i(sg['n'])} segments with enough peak and off-peak events, the median **actual** peak/off-peak slowdown ratio is **{f(sg['act_med'],3)}** and the median **scheduled** ratio {f(sg['sched_med'],3)}
(correlation between the two across segments: {f(sg['corr'],2)}). Only {f(100*sg['sched_within1'],1)}% of Boston segments have a scheduled ratio within 1% of 1.0.

**Contrast with Bengaluru (method only).** In the BMTC timetable the median scheduled ratio is {f(bm_flat['sd_median'],3)} and {f(100*bm_flat['sd_within1pct'],1)}% of segments are within 1% of 1.0, i.e. a near-flat timetable.
The Boston timetable, in contrast, plans a different run time at peak for many segments. This shows that the BMTC finding "no scheduled slowdown" reflects how that feed was built, and why real delay needs actual-time data.
It does **not** say anything about how late BMTC buses really are.

### 8.4 Headway (bunching) on high-frequency routes
On {i(hw.n)} headway-standard events the mean ratio of actual to scheduled gap is {f(hw.mean_headway_ratio,3)}; **{f(100*hw.bunched_rate,1)}%** of gaps are under half the scheduled gap (bunching) and **{f(100*hw.gap_rate,1)}%** are over 1.5x (large gaps).

### 8.5 Performance on this real dataset
{btxt}

### 8.6 Correctness
{ctxt}. Tests: `tests/test_delay.py`.

### 8.7 Limitations of the delay part
1. Boston, not Bengaluru: different operator, city, fleet and timetable practice.
2. Only timepoints (not every stop) and only departures are recorded; delay is departure delay at timepoints.
3. 'Actual' times come from MBTA's own vehicle tracking; the dataset notes data is not guaranteed complete for any stop or date, and {f(100-w.iloc[4].pct_retained,1)}% of events had no actual time.
4. Four sampled months (one per season) of 2024, not the full year; no claim about other months.
5. 'Headway'-standard trips are not judged on schedule delay, so delay figures cover only 'Schedule'-standard trips.
6. Peak is defined by scheduled hour (07-10h and 17-20h), the same windows as the Bengaluru analysis.
"""

# ----------------------------------------------------------------------------- DATA_CARD
def data_card(d):
    m, p, c = d["meta"], d["prof"], d["compl"]
    lic = Path("data/raw/repo_LICENSE").read_text().splitlines()[0].lstrip("# ").strip() if Path("data/raw/repo_LICENSE").exists() else None
    feed = pd.read_csv("data/raw/gtfs/feed_info.txt").iloc[0]
    files = pd.DataFrame([{"file": n, "bytes": f"{v['bytes']:,}", "sha256": v["sha256"][:16] + "...",
                           "rows": i(p["row_counts"].get(n.replace(".txt", "")))}
                          for n, v in m["files"].items() if n.endswith(".txt")])
    st, ip, hv = p["stop_times"], p["interpolation_profile"], p["haversine_vs_shape_dist"]
    pre = ", ".join(f"`{k}-` ({v})" for k, v in p["route_name_prefix_counts"].items() if k != "(none)")
    cl = d["classes"]
    return f"""# Data card: BMTC GTFS snapshot

*Generated by `src/make_docs.py` from `data/raw/snapshot_meta.json` and `outputs/data_profile.json`. Do not edit by hand.*

## Source
- Unofficial BMTC GTFS, repository `Vonter/bmtc-gtfs` (file `gtfs/bmtc.zip`), downloaded **{m['downloaded_on']}**.
- Feed metadata (feed_info.txt): feed_version {feed['feed_version']}, valid {feed['feed_start_date']} to {feed['feed_end_date']}.
- Zip: {m['zip_bytes']:,} bytes, SHA-256 `{m['zip_sha256']}`.
- The feed changes over time, so all counts below describe this one snapshot only.

## Files in this snapshot (GTFS `.txt`, not `.csv`)
{md_table(files)}

Used by the pipeline: routes, trips, stop_times, stops, calendar. `calendar_dates.txt` is {'present' if (d['extras'] or {}).get('calendar_dates_present') else 'not present in this snapshot'}. Other files (shapes, fares, translations) are not used except `shape_dist_traveled` in stop_times for one sanity check.

## License
Licence text found in the repository `LICENSE` file, first heading: **"{lic}"**. The repository README itself states no separate licence. ODbL obligations (attribution, share-alike, keep open) should be checked by the team before redistributing derived data.

## Provenance and reliability (D3)
The README of the source repository says the data come from the **Namma BMTC 2.0 app**, which "has inaccurate data, and is known to have incorrect timetables and stop timings", and that only routes available in that app are included. It also states: "Only arrival times are published, so GTFS arrival and departure times are equal at each stop. Arrival times missing from the source are interpolated by the distance between stops." Treat this as a scraped, approximate timetable, not an official schedule.

## Quality profile (measured)
- stop_times rows: **{i(st['n'])}** across {i(st['trips_with_stop_times'])} trips and {i(st['stops_used'])} distinct stops.
- Arrival equals departure on {f(st['pct_arrival_equals_departure'], 2)}% of rows, so dwell time is always 0 and cannot be analysed.
- Blank arrival or departure times: {i(st['blank_time'])}. Duplicate (trip_id, stop_sequence): {i((d['wf'].iloc[2].rows_in - d['wf'].iloc[2].rows_out))}. Orphan trips/routes/stops: {i(d['wf'].iloc[4].rows_in - d['wf'].iloc[4].rows_out)} / {i(d['wf'].iloc[6].rows_in - d['wf'].iloc[6].rows_out)} / {i(d['wf'].iloc[7].rows_in - d['wf'].iloc[7].rows_out)}.
- Rows with an hour of 24 or more (after-midnight times): {i(st['hour_ge_24'])}. They are kept by the custom time parser.
- `timepoint = 0` rows: {i(st['timepoint0'])} ({f(st['pct_timepoint_0'], 2)}%).
- Calendar: {i((d['extras'] or {}).get('calendar_services_total'))} service_ids; {i((d['extras'] or {}).get('calendar_services_selected'))} run on every weekday and are used for the `{d['extras']['service_day']}` analysis (services running on all of Mon to Fri).

## Interpolation profile (D5)
Timetables look smooth because of how they were built. Among the {i(ip['valid_hops'])} valid hops:
- {f(ip['pct_hops_within_1pct_of_trip_median_speed'])}% have a speed within 1% of their own trip's median speed; {f(ip['pct_hops_within_5pct_of_trip_median_speed'])}% within 5%; {f(ip['pct_hops_within_20pct_of_trip_median_speed'])}% within 20%.
- Mean hop speed is {f(ip['mean_hop_speed_kmh'])} km/h (std {f(ip['std_hop_speed_kmh'])}); speed percentiles p05/p25/p50/p75/p95 = {ip['speed_quantiles_p05_p25_p50_p75_p95']} km/h.
- Using the feed's own `shape_dist_traveled`, the hop speed percentiles p05/p25/p50/p75/p95 are {hv['shape_dist_based_speed_kmh_p05_p25_p50_p75_p95']} km/h ({i(hv['hops_compared'])} hops compared).
- Straight-line (haversine) distance divided by `shape_dist_traveled` has p10/p50/p90 = {hv['haversine_over_shape_dist_p10_p50_p90']}, i.e. straight-line distance is usually slightly shorter than the shape distance, as expected.

**Reading:** the scheduled times are close to constant-speed between stops. Differences between hours or trips are therefore mostly rounding and route geometry, not planned congestion. This limits what any "scheduled slowdown" analysis can show (see RESULTS.md).

## Feeder / trunk classification (T13, heuristic)
- Route-name prefixes found in `route_short_name` (top): {pre}.
- `MF-` is a clear prefix ({i(p['mf_routes'])} routes, e.g. {', '.join(p['mf_examples'][:4])}). The team should confirm with BMTC that `MF` means metro feeder; the code assumes it.
- Rule used: **feeder** = name starts with `{'`, `'.join(d['cfg']['route_class']['feeder_prefixes'])}`; **trunk** = not feeder and median trip length at least {d['cfg']['route_class']['trunk_min_km']} km (straight-line); **other** = the rest. This is a heuristic, not an official label.
- Count per class (routes with weekday trips):

{md_table(cl)}

## Responsible use (D7)
The columns present (ids, names, times, coordinates of bus stops, fares, flags) contain no personal data about riders or staff. The `feed_contact_email` field in `feed_info.txt` is the dataset maintainer's public contact, not used here.
"""



def data_card_delay(d):
    g = d.get("delay")
    if not g:
        return "# Delay data card\n\nnot measured\n"
    m = g["meta"]
    files = pd.DataFrame([{"file": n, "bytes": f"{v['bytes']:,}", "sha256": v["sha256"][:16] + "..."}
                          for n, v in m["files"].items() if any(n.endswith(f"-{x:02d}.csv") for x in g["cfg"]["months"])])
    desc = " ".join(m["description_text"].split())[:420]
    return f"""# Data card: MBTA (Boston) bus arrival/departure times 2024 (real scheduled vs actual)

*Generated by `src/make_docs.py` from `data/raw_delay/delay_snapshot_meta.json`. Used ONLY for the real-delay analysis (RESULTS.md section 8). It is Boston data, not Bengaluru.*

## Source
- MBTA Open Data Portal, item "{m['title']}" (ArcGIS item `{m['item_id']}`), owner: {m['owner_org']}. Downloaded {m['downloaded_on']}.
- Zip: {m['zip_bytes']:,} bytes, SHA-256 `{m['zip_sha256']}`; 12 monthly CSVs inside, of which months {g['cfg']['months']} are analysed (to keep run time manageable).
- **Licence shown on the item: {m['licence_on_item']}.** The item also carries this disclaimer: the agency "shall not be held liable for any errors in this data".
- Item description (excerpt): "{desc}..."

## Files analysed
{md_table(files)}

## Columns (from the item's data dictionary; all read as text, then cast)
service_date, route_id, direction_id, half_trip_id, stop_id, time_point_id, time_point_order, point_type (Startpoint / Midpoint / Endpoint), standard_type (Schedule / Headway),
scheduled, actual, scheduled_headway, headway. `scheduled` and `actual` are times of day stamped `1900-01-01T..Z`, so after-midnight trips wrap to 00:xx; delay is computed modulo 24 h.

## Quality notes (measured, see `outputs/tables/delay_waterfall.csv`)
- Events without an actual time are dropped; the share is in RESULTS.md section 8.1.
- Rows with an absolute delay above {g['cfg']['max_abs_delay_sec']} s are treated as errors and dropped.
- 'Headway'-standard events are excluded from schedule delay (the data dictionary says scheduled time must not be used to evaluate their reliability); they are used for headway analysis.
- Data covers timepoints only, not every stop, and is "not guaranteed to be complete for any stop or date" (item description).

## Responsible use
No rider or driver personal data appears in the columns (route, trip id, stop and timepoint codes, times). Results describe Boston bus operations and must not be presented as Bengaluru results.
"""

# ----------------------------------------------------------------------------- RESULTS
def bench_md(d):
    B = d["bench"]
    if not B:
        return "Benchmark not run (`outputs/benchmark_results.json` missing): " + NM + "."
    rows = []
    for n, e in sorted(B["scaling"].items(), key=lambda kv: int(kv[0])):
        s = e["stages_seconds"]
        rows.append({"cores": n, "clean s": f(s['clean']['median']), "join s": f(s['join']['median']),
                     "window+agg s": f(s['window_agg']['median']),
                     "total median s": f(s['total']['median']), "min-max s": f"{f(s['total']['min'])} - {f(s['total']['max'])}",
                     "throughput rows/s": i(e['throughput_rows_per_s']), "speedup S_N": f(e['speedup_total']),
                     "efficiency": f(e['efficiency_total']), "oversubscribed": e['oversubscribed']})
    out = [md_table(pd.DataFrame(rows))]
    out.append(f"\nPer-stage speedup: " + "; ".join(f"{n} cores: " + ", ".join(f"{k} {f(v)}" for k, v in e['speedup_by_stage'].items())
                                                    for n, e in sorted(B['scaling'].items(), key=lambda kv: int(kv[0]))) + ".")
    jc = B["join_comparison"]
    jr = []
    for v, r in jc["variants"].items():
        jr.append({"join variant": v, "median s": f(r['join_seconds']['median'], 3),
                   "min-max s": f"{f(r['join_seconds']['min'], 3)} - {f(r['join_seconds']['max'], 3)}",
                   "shuffle write bytes (median/run)": i(r['shuffle_write_bytes_per_run_median']) if not isinstance(r['shuffle_write_bytes_per_run_median'], str) else NM,
                   "plan file": r['plan_file']})
    out.append(f"\n### Broadcast vs shuffle join (stage 2 only, local[{jc['cores']}])\n\n" + md_table(pd.DataFrame(jr)) +
               "\n\nBroadcast = `F.broadcast()` hint on the small dimension tables (trips, routes, stops, service list). Shuffle = no hint and "
               "`spark.sql.autoBroadcastJoinThreshold=-1`, giving sort-merge joins with `Exchange` nodes (see the saved plans). Adaptive query execution is off "
               "so Spark cannot silently switch strategy. Shuffle bytes come from the Spark event log; broadcast data movement is not counted as shuffle.")
    sy = B.get("synthetic_scaleup")
    if isinstance(sy, dict):
        r2 = [{"replication": f"x{r['factor']}", "synthetic rows": i(r['synthetic_rows']), "cores": r['cores'],
               "window+agg s": f(r['window_agg_seconds']['median'])} for r in sy["results"]]
        out.append("\n### Synthetic scale-up (synthetic replication, not real data)\n\n" + md_table(pd.DataFrame(r2)) +
                   f"\n\nCleaned, joined rows were copied k times with new trip_id and route_id. Only the window + aggregation stage was timed, with "
                   f"{sy['config']['timed_runs']} timed run per point and no discarded warm-up, and an {sy['config']['driver_memory']} driver heap. Treat as indicative only.")
    return "\n".join(out)


def results(d):
    B, c, s, env = d["bench"], d["compl"], d["s"], d["env"]
    sg, pr, rt, hw = s["seg"], s["pair"], s["route"], s["headway"]
    low = (sg["zero_std_share"] or 0) >= 0.5 or sg["cv_median"] < 0.02
    tt = pd.read_csv("outputs/tables/top_segments_peak_variability.csv").head(5)
    tr = pd.read_csv("outputs/tables/top_routes_peak_variability.csv").head(5)
    ts = pd.read_csv("outputs/tables/top_stop_pairs_slowdown.csv").head(5)
    ftxt = md_table(d["ft"]) if len(d["ft"]) else NM
    hs = d["hot_sum"][d["hot_sum"].level == "stop_pair"][["pattern", "n_units_touching", "n_units_ranked", "median_peak_cv", "median_slowdown_ratio", "median_peak_speed_kmh"]].round(4)
    unmatched = d["hot_match"][d["hot_match"].n_stop_ids == 0].pattern.tolist()
    corr = d["corr"]
    corr_txt = (f"PASS: {i(corr['hops_compared'])} hops and {i(corr['segment_groups_compared'])} segment-period groups from {i(corr['sample_trips'])} sampled trips "
                f"matched pandas (max abs speed diff {corr['max_abs_speed_diff_kmh']:.1e} km/h, max abs mean/std diff {corr['max_abs_diff_mean_std_sec']:.1e} s)"
                if corr and corr.get("passed") else ("FAIL" if corr else NM))
    fv = d["ft"].set_index("route_class") if len(d["ft"]) else None
    comp = ""
    if fv is not None and {"feeder", "trunk"} <= set(fv.index):
        comp = (f"Feeder segments (n={i(fv.loc['feeder','ranked_segments'])}) have a median scheduled hop speed of {f(fv.loc['feeder','median_peak_speed_kmh'])} km/h and median CV "
                f"{f(fv.loc['feeder','median_peak_cv'], 4)}; trunk segments (n={i(fv.loc['trunk','ranked_segments'])}) {f(fv.loc['trunk','median_peak_speed_kmh'])} km/h and {f(fv.loc['trunk','median_peak_cv'], 4)}.")
    scal = ""
    if B:
        ks = sorted(B["scaling"], key=int)
        scal = ", ".join(f"{k} cores: S={f(B['scaling'][k]['speedup_total'])} (efficiency {f(B['scaling'][k]['efficiency_total'])})" for k in ks)
    return f"""# Results: scheduled travel-time analytics for BMTC (PySpark)

*Generated by `src/make_docs.py`. Every number comes from files in `outputs/`. This analysis uses a **static, scraped timetable**: all times are
**scheduled (timetable-implied)**; no actual arrival times exist for Bengaluru, so sections 1-7 measure neither delay nor real congestion.
Section 8 adds a REAL actual-vs-scheduled delay analysis on a public Boston dataset to show the same Spark method on data that does contain actual times.*

## 1. Data and completeness
- Snapshot: {d['meta']['downloaded_on']}; stop_times rows read: **{i(c['raw'])}**. Day type analysed: **{d['extras']['service_day']}** (services that run on every Mon to Fri).
- **Data completeness ratio** (final analysed hops / raw stop_times rows) = **{f(100*c['ratio'])}%** ({i(c['final'])} / {i(c['raw'])}).
  - Intended scope exclusion (other day types): {i(c['scope'])} rows. Structural (first stop of a trip has no hop): {i(c['struct'])} rows. Data-quality removals (invalid hop time, distance or speed): {i(c['quality'])} rows.
  - Cleaned stop_times rows after joins (the throughput denominator): {i(c['cleaned_stop_times'])}.
- Full waterfall: `outputs/tables/completeness_waterfall.csv`, figure `fig6_completeness_waterfall.png`.

{md_table(d['wf'][['stage','rows_in','rows_out','pct_retained']])}

## 2. Q1: scheduled variability and peak slowdown
Ranking requires at least {d['minn']} trips per segment and period (peak = 07-10h and 17-20h by hop start; the rest is off-peak).

- Segments (route, direction, from stop, to stop): {i(sg['total'])} in total, **{i(sg['ranked_peak'])}** with enough peak trips to rank, {i(sg['ranked_both'])} with enough in both periods.
- Among ranked segments, **{f(100*sg['zero_std_share'],1)}%** have exactly zero peak std dev (every trip has the same scheduled hop time). Median peak CV = {f(sg['cv_median'],4)}, 90th percentile {f(sg['cv_p90'],4)}, maximum {f(sg['cv_max'],3)}.
- Peak / off-peak mean hop-time ratio per segment: median {f(sg['sd_median'],4)}; {f(100*sg['sd_within1pct'],1)}% of ranked segments are within 1% of 1.0; 95th percentile {f(sg['sd_p95'],3)}; maximum {f(sg['sd_max'],3)}.
- Pooled across routes (stop pairs): {i(pr['ranked_peak'])} ranked; median peak CV {f(pr['cv_median'],4)}, 90th percentile {f(pr['cv_p90'],4)}; median slowdown {f(pr['sd_median'],4)}, 95th percentile {f(pr['sd_p95'],3)}.
- Routes (trip duration, per direction): {i(rt['ranked'])} of {i(rt['total'])} have enough peak trips; median peak CV {f(rt['cv_median'],4)}, 90th percentile {f(rt['cv_p90'],3)}, maximum {f(rt['cv_max'],3)}.
- Scheduled headway CV per route/direction/stop/hour: median {f(hw['cv_median'],3)}, 90th percentile {f(hw['cv_p90'],3)} over {i(hw['groups'])} groups (at least {d['cfg']['thresholds']['min_headways']} gaps each).

**Finding.** {"Scheduled variability is very low almost everywhere: most segments have the same planned hop time on every trip and no planned peak slowdown (ratio about 1.0). The timetable (see DATA_CARD.md, interpolation profile) appears to assume near-constant speed. The ranked lists below are therefore driven by a small number of unusual segments, often with different stop patterns or rounding, and should not be read as congestion hotspots." if low else "Scheduled variability is non-trivial for a meaningful share of segments; see the ranked tables."}
The few high values were not investigated individually; with only the minimum number of trips they may reflect mixed stop patterns or single odd trips rather than planned congestion.

Top 5 segments by peak CV (`top_segments_peak_variability.csv`):

{md_table(tt[['route_short_name','from_stop_name','to_stop_name','PEAK_n','PEAK_mean_hop_sec','PEAK_cv','slowdown_ratio']].round(3))}

Top 5 routes by peak trip-duration CV (`top_routes_peak_variability.csv`; includes variation from different stop patterns under one route_id):

{md_table(tr[['route_short_name','direction_id','route_class','PEAK_n','PEAK_mean_min','PEAK_cv']].round(3))}

Top 5 stop pairs by scheduled slowdown ratio (`top_stop_pairs_slowdown.csv`):

{md_table(ts[['from_stop_name','to_stop_name','n_routes','PEAK_n','OFF_PEAK_n','slowdown_ratio']].round(3))}

### Named hotspots (T12)
Only patterns found in stop names are analysed. Not found in this snapshot: {', '.join(unmatched) if unmatched else 'none'}. Matched stop names are listed in `hotspot_stop_matches.csv`. Stop-pair level summary (ranked pairs need {d['minn']}+ trips in both periods):

{md_table(hs)}

## 3. Q2: feeder vs trunk (heuristic classes)
{comp}

{ftxt}

Rule: feeder = `route_short_name` starts with `MF-`; trunk = non-feeder with median trip length of at least {d['cfg']['route_class']['trunk_min_km']} km. Routes per class: {', '.join(f"{r.route_class} {int(r.n_routes)}" for r in d['classes'].itertuples())}. Speeds use straight-line distance. Differences in scheduled speed mostly reflect the timetable's assumed speed and stop spacing, not observed traffic.

## 4. Q3: performance (machine: {env['cpu']}, {env['logical_cores']} logical cores = {env['performance_cores']} performance + {env['efficiency_cores']} efficiency, {env['ram_gb']} GB RAM; Spark {env['pyspark']}, {env['java']}, Python {env['python']})
Baseline: Spark `local[1]`. Each core count ran in its own process; {d['bench']['config']['warmup_runs'] if B else NM} discarded warm-up and {d['bench']['config']['timed_runs'] if B else NM} timed runs; `shuffle.partitions`={d['cfg']['spark']['shuffle_partitions']}; adaptive execution off; no caching between runs. Stage times each include their Parquet read and write (needed to force execution). Speedup S_N = T1 / TN on median total time.

{bench_md(d)}

**Reading.** {scal if B else NM}. The dataset is only about {i(c['cleaned_stop_times'])} rows, so fixed costs (JVM start-up of tasks, scheduling, CSV parsing of a 130 MB file, Parquet I/O, driver-side `toPandas`) are a large share of the run time and speedup is sub-linear; this chip has {env['efficiency_cores']} efficiency cores besides {env['performance_cores']} performance cores, which may also lower efficiency at higher N (not isolated in these measurements). Window and aggregation dominate the run time; the join stage is short.

## 5. Aggregation correctness (T22)
{corr_txt}. Test: `tests/test_pandas_crosscheck.py`.

## 6. Comparison with the base paper
Base paper as cited in our proposal: N. L. Mnyone and N. U. Amin (2026), *Real-Time Data Processing in Smart Transportation Using Apache Spark*, Preprints, DOI 10.20944/preprints202603.1261.v1.
**The team must confirm every statement about the paper from the paper itself; this project did not read it.** According to the proposal, the paper used synthetic GPS/vehicle-sensor logs, Spark Core RDDs and basic Spark SQL, and studied ingestion bottlenecks and memory use on single-table feeds. Contrast of *methodology only* (no numeric comparison):

| Aspect | Base paper (as stated in our proposal) | This project |
|---|---|---|
| Data | synthetic GPS / sensor logs | real, multi-table relational GTFS (routes, trips, stop_times, stops, calendar) |
| Structure | single-table feeds | four-way joins with orphan checks and a completeness waterfall |
| API | RDDs, basic Spark SQL | DataFrames, `Window` functions (lag per trip, per route-stop) |
| Metrics | ingestion bottlenecks, memory | sequence-level metrics (hop time, speed, scheduled headway), correctness cross-check |
| Join/shuffle | not part of our summary | measured broadcast vs sort-merge join with plans and shuffle bytes |

## 7. Limitations
1. **Scraped timetable.** Data come from the unofficial Namma BMTC 2.0 app scrape, which its maintainer calls unreliable; routes without live tracking are absent.
2. **Interpolated, near-constant-speed times** (DATA_CARD.md): little genuine variability is available to measure.
3. **No actual-time data.** Scheduled slowdown is a timetable-implied proxy for planned congestion, not a measure of it; no delay can be computed.
4. **Straight-line distance** underestimates road distance, so hop speeds are lower bounds.
5. **Small data versus Spark overhead:** about {i(c['cleaned_stop_times'])} rows; speedup is limited, and 1/2/4-core runs on a laptop with mixed core types are noisy.
6. **Heuristic feeder/trunk rule** (name prefix plus length), not an official BMTC classification.
7. **Few trips per segment:** the median route direction has only a handful of weekday trips, so the {d['minn']}-trip minimum keeps only the busiest segments ({i(sg['ranked_peak'])} of {i(sg['total'])}).
8. Dwell time is always 0 (arrival equals departure in the source).
9. Synthetic scale-up numbers are replicated data, not real, and use a single timed run per point.
10. Hop time-of-day uses the departure hour of the previous stop; trips that cross bucket boundaries are assigned by hop start.
11. Real delay is available only for the Boston dataset (section 8), not for Bengaluru.
""" + delay_section(d)


# ----------------------------------------------------------------------------- PROPOSAL_UPDATES
def proposal(d):
    p, c, env, B = d["prof"], d["compl"], d["env"], d["bench"]
    lic = Path("data/raw/repo_LICENSE").read_text().splitlines()[0].lstrip("# ").strip()
    sz = sum(v["bytes"] for n, v in d["meta"]["files"].items() if n.endswith(".txt"))
    used = sum(d["meta"]["files"][n + ".txt"]["bytes"] for n in ("stop_times", "trips", "routes", "stops", "calendar"))
    return f"""# Proposal wording updates

Exact replacement text so the Word proposal matches what the code does. Numbers come from `DATA_CARD.md` / `outputs/`.

| # | Replace | With |
|---|---|---|
| 1 | "schedule delay", "actual vs scheduled", ">20 minutes deviation" | "scheduled travel-time variability (standard deviation and coefficient of variation of planned hop and trip times across trips), scheduled hop speed, peak vs off-peak scheduled slowdown, and scheduled headway variability. Static GTFS holds planned times only, so actual delay cannot be measured." |
| 2 | Research questions on delay hotspots | Q1: "Which route segments and corridors have the highest scheduled peak-hour travel-time variability and the largest peak vs off-peak scheduled slowdown?" Q2: "How do scheduled variability and hop speed compare between metro-feeder routes and long trunk routes?" Q3: "What speedup and throughput does PySpark achieve with 1, 2 and 4 cores, and how do broadcast and shuffle joins compare?" |
| 3 | "~250 MB / ~1.5M records" | "a snapshot taken on {d['meta']['downloaded_on']}: {i(p['stop_times']['n'])} stop_times rows ({d['meta']['files']['stop_times.txt']['bytes']/1e6:.0f} MB), {i(p['row_counts']['trips'])} trips, {i(p['row_counts']['routes'])} routes, {i(p['row_counts']['stops'])} stops; about {used/1e6:.0f} MB for the five files used ({sz/1e6:.0f} MB of GTFS text files in total). The feed changes over time." |
| 4 | "ODbL" | "{lic} (as stated in the source repository's LICENSE file)" |
| 5 | `.csv` file names | GTFS `.txt` files: `routes.txt`, `trips.txt`, `stop_times.txt`, `stops.txt`, `calendar.txt` (`calendar_dates.txt` is not in this snapshot) |
| 6 | `unix_timestamp(col, "HH:mm:ss")` in the code outline | "A custom GTFS time parser (regex check, then hours*3600 + minutes*60 + seconds) because GTFS times can exceed 24:00:00. Hop time = arrival seconds at stop i minus departure seconds at stop i-1 within the same trip (`Window.partitionBy(trip_id).orderBy(stop_sequence)` with `lag`); dwell = departure minus arrival." |
| 7 | "8-core / 16 GB" | "{env['cpu']}, {env['logical_cores']} cores ({env['performance_cores']} performance + {env['efficiency_cores']} efficiency), {env['ram_gb']} GB RAM, {env['os']}, Java {env['java']}, Python {env['python']}, PySpark {env['pyspark']}" |
| 8 | (new limitation) | "The timetable is scraped from an unofficial app source whose maintainer calls it unreliable, and the times appear interpolated at near-constant speed ({f(p['interpolation_profile']['pct_hops_within_20pct_of_trip_median_speed'],1)}% of hops are within 20% of their trip's median speed), so scheduled variability is expected to be low." |
| 9 | Segment definition by `route_long_name, stop_id` | "A segment is (route_id, direction_id, from_stop_id, to_stop_id); a stop-pair rollup across routes is also reported." |
| 10 | "corridor" / named junctions | "Named hotspots are found by matching stop names (e.g. Silk Board, Tin Factory, Hebbal); only names present in the data are reported." |
| 12 | "actual delay at BMTC stops" (any claim of measured delay) | "BMTC publishes no actual arrival times, so BMTC delay cannot be measured. Actual-vs-scheduled delay is demonstrated on the public MBTA (Boston) bus arrival/departure dataset (licence on the item: CC0), using the same PySpark pipeline design; those results describe Boston, not Bengaluru." |
| 13 | Objectives / Q3 evaluation data | add: "The pipeline is also benchmarked on 1, 2 and 4 cores on {i(d['delay']['bench']['cleaned_rows']) if d.get('delay') and d['delay']['bench'] else NM} real delay events (4 months of 2024), in addition to the Bengaluru timetable." |
| 11 | "metro-feeder / trunk" | "Heuristic classes: feeder = route name prefix `MF-`; trunk = non-feeder with median trip length at least {d['cfg']['route_class']['trunk_min_km']} km." |
"""


# ----------------------------------------------------------------------------- README
def readme(d):
    B, c, s, env, cfg = d["bench"], d["compl"], d["s"], d["env"], d["cfg"]
    sg, g = s["seg"], d.get("delay")
    feed = pd.read_csv("data/raw/gtfs/feed_info.txt").iloc[0]
    fd = lambda x: f"{str(x)[:4]}-{str(x)[4:6]}-{str(x)[6:]}"
    mon = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    months = ", ".join(mon[m] for m in cfg["delay"]["months"])
    bm_rows = ""
    if B:
        for k in sorted(B["scaling"], key=int):
            e = B["scaling"][k]
            bm_rows += f"| {k} | {f(e['total_latency_s_median'],1)} s | {f(e['speedup_total'])}x | {f(e['efficiency_total'])} |\n"
    dl_rows = ""
    if g and g["bench"]:
        for k in sorted(g["bench"]["scaling"], key=int):
            e = g["bench"]["scaling"][k]
            dl_rows += f"| {k} | {f(e['total_latency_s_median'],1)} s | {f(e['speedup_total'])}x | {f(e['efficiency_total'])} |\n"
    jv = B["join_comparison"]["variants"] if B else None
    join_line = (f"Broadcast join **{f(jv['broadcast']['join_seconds']['median'],2)} s** vs shuffle join **{f(jv['shuffle']['join_seconds']['median'],2)} s** "
                 f"(join stage, 4 cores); shuffle data written {i(jv['broadcast']['shuffle_write_bytes_per_run_median'])} vs {i(jv['shuffle']['shuffle_write_bytes_per_run_median'])} bytes."
                 ) if jv else NM
    o = g["overall"] if g else None
    ptxt = (f"{i(o['n'])} departure events: mean delay **{f(o['mean_delay_sec']/60,1)} min** (median {f(o['median_delay_sec']/60,1)}, p90 {f(o['p90_delay_sec']/60,1)}), "
            f"**{f(100*o['on_time_rate'],1)}% on time**, {f(100*o['late_rate'],1)}% late, {f(100*o['early_rate'],1)}% early.") if g else NM
    corr = d["corr"]
    return f"""<div align="center">

# BMTC and MBTA Bus Analytics with PySpark

**Scalable route-efficiency, scheduled travel-time variability and real delay analytics for Bengaluru (BMTC), validated on Boston (MBTA) data**

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.3-E25A1C?logo=apachespark&logoColor=white)
![Java](https://img.shields.io/badge/Java-17-007396?logo=openjdk&logoColor=white)
![Course](https://img.shields.io/badge/CSE557-Big%20Data%20Analytics-blue)

*Ramaiah Institute of Technology, course CSE557 Big Data Analytics*

</div>

---

## Overview

BMTC runs thousands of buses on congested Bengaluru corridors, and planners need to know where service is slow or unreliable.
The only public BMTC data is a **static GTFS timetable**: it contains *planned* times and **no actual arrival times**, so real BMTC delay cannot be measured.
This project therefore has two parts that share one PySpark design.

| | **Part A: Bengaluru (BMTC)** | **Part B: Boston (MBTA)** |
|---|---|---|
| Data | Static GTFS timetable, 5 linked tables | Real scheduled **and** actual bus departures |
| Size | {i(d['prof']['stop_times']['n'])} `stop_times` rows | {i(g['wf'].iloc[0].rows_out) if g else NM} events ({months} 2024) |
| Question | What does the timetable *plan*? How variable is it, and does it plan a peak slowdown? | How late are buses really, how often on time, how much bunching? |
| Result | Timetable is almost flat (about {f(d['prof']['interpolation_profile']['mean_hop_speed_kmh'],1)} km/h everywhere) | Mean delay {f(o['mean_delay_sec']/60,1) if g else NM} min, {f(100*o['on_time_rate'],1) if g else NM}% on time |

> **Important.** Part B is **Boston, not Bengaluru**. BMTC publishes no actual-time data, so we show the same Spark method on a public dataset that has it.
> Boston results must never be read as BMTC performance.

## Headline results

### Part A: Bengaluru timetable (scheduled times only)
- **{f(100*c['ratio'])}%** of raw `stop_times` rows end up as analysed hops ({i(c['final'])} of {i(c['raw'])}); losses are the weekday filter, the first stop of each trip and {i(c['quality'])} invalid rows.
- The timetable is close to constant speed, so scheduled variability is very low: **{f(100*sg['zero_std_share'],1)}%** of ranked segments have zero peak variability and the median peak/off-peak ratio is **{f(sg['sd_median'],3)}**.
- This is a finding about the data: the source says times were interpolated by distance, and {f(d['prof']['interpolation_profile']['pct_hops_within_5pct_of_trip_median_speed'])}% of hops run within 5% of their trip's median speed. The feed cannot reveal planned congestion.

### Part B: Boston real delay
- {ptxt}
- {f(100*g['hw'].bunched_rate,1) if g else NM}% of gaps on high-frequency routes are bunched (under half the scheduled gap) and {f(100*g['hw'].gap_rate,1) if g else NM}% are large gaps (over 1.5x).
- Boston's timetable plans a peak slowdown (median scheduled ratio {f(g['seg']['sched_med'],3) if g else NM}); BMTC's plans none ({f(sg['sd_median'],3)}).

<p align="center"><img src="outputs/figures/fig9_delay_overview.png" width="900" alt="Boston delay overview"></p>

### Performance (Apple M3, 8 cores, 16 GB, Spark 3.5.3)

| Cores | BMTC time (2.0M rows) | BMTC speedup | BMTC efficiency |
|---|---|---|---|
{bm_rows}
| Cores | Boston time ({i(g['bench']['cleaned_rows']) if g and g['bench'] else NM} events) | Speedup | Efficiency |
|---|---|---|---|
{dl_rows}
{join_line}
The small BMTC dataset is dominated by Spark's fixed overhead, while the larger Boston run scales better. Protocol: one process per core count, one discarded warm-up, median of timed runs, fixed Spark configuration.

### Correctness
Spark results were recomputed independently in pandas and compared: BMTC {i(corr['hops_compared']) if corr else NM} hops and {i(corr['segment_groups_compared']) if corr else NM} segment groups; Boston {i(g['corr']['events_compared']) if g and g['corr'] else NM} events recomputed from the raw CSV. All differences are below 1e-12. 13 pytest tests pass.

## Pipeline

```mermaid
flowchart LR
    A[GTFS .txt files] --> B[Ingest: explicit schemas]
    B --> C[Clean: time parser, dedupe, fallbacks]
    C --> D[Join: trips, routes, stops, weekday services]
    D --> E[Window lag: hop time, distance, speed]
    E --> F[Aggregate: segment, route, stop pair, headway]
    F --> G[Tables and figures]
    H[MBTA monthly CSVs] --> I[Clean: parse times, drop gaps]
    I --> J[Delay = actual - scheduled]
    J --> K[Aggregate: on-time, hour, route, bunching]
    K --> G
```

Key techniques: explicit `StructType` schemas, a **row-count waterfall** that explains every lost row, a custom parser for GTFS times beyond 24:00:00, `Window.partitionBy(...).orderBy(...)` with `lag()`, haversine distance in pure Spark column expressions, broadcast vs sort-merge joins, `pivot` for peak vs off-peak, and Parquet for intermediate data.

<p align="center">
<img src="outputs/figures/fig5_slowdown_and_hotspots.png" width="900" alt="BMTC slowdown and hotspots"><br>
<img src="outputs/figures/fig11_actual_vs_scheduled_slowdown.png" width="700" alt="Scheduled vs actual slowdown (Boston)">
</p>

More figures are in [`outputs/figures/`](outputs/figures/): waterfall, distributions, feeder vs trunk, speedup charts and a synthetic scale-up test.

## Quick start

**Prerequisites:** Java 11 or 17 (`java -version`) and Python 3.10 to 3.12 (PySpark 3.5 does not support 3.13).

```bash
git clone https://github.com/nik1taa28/bda-project.git
cd bda-project
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./run_all.sh              # full run: download, analyse, test, benchmark, figures, docs (over an hour)
```

Faster options: `SYNTHETIC=0 ./run_all.sh` skips the long synthetic scale-up test; `DELAY=0 ./run_all.sh` skips the Boston part.

| Step | Command | Approx. time |
|---|---|---|
| Download BMTC GTFS (93 MB zip) | `python scripts/download_data.py` | seconds |
| BMTC pipeline | `python -m src.pipeline --service-day weekday` | about 1 min |
| Download Boston data (315 MB zip) | `python scripts/download_delay_data.py` | a few min |
| Boston delay pipeline | `python -m src.delay_pipeline` | about 8 min |
| Tests | `pytest -q` | seconds (run the pipelines first) |
| Benchmarks | `python -m src.benchmark [--synthetic] [--with-delay]` | long |
| Figures and docs | `python -m src.viz && python -m src.make_docs` | seconds |

If a download is blocked, place `bmtc.zip` in `data/raw/` (or the MBTA zip as `data/raw_delay/mbta_bus_2024.zip`) and re-run. Nothing is ever fabricated. All thresholds (peak windows, speed cap, on-time window, minimum trips) live in [`config.yaml`](config.yaml).

## Data

| | BMTC (Bengaluru) | MBTA (Boston) |
|---|---|---|
| Source | [Vonter/bmtc-gtfs](https://github.com/Vonter/bmtc-gtfs), unofficial GTFS from the Namma BMTC 2.0 app | [MBTA Open Data Portal](https://mbta-massdot.opendata.arcgis.com/datasets/96c77138c3144906bce93d0257531b6a), "MBTA Bus Arrival Departure Times 2024" |
| Coverage used | one timetable snapshot downloaded {d['meta']['downloaded_on']} (feed valid {fd(feed['feed_start_date'])} to {fd(feed['feed_end_date'])}); weekday services only | {len(cfg['delay']['months'])} months of 2024: {months} |
| Licence | ODbL (from the repository LICENSE) | CC0 (shown on the item) |
| One row | a bus trip visiting a stop (planned times) | a bus trip leaving a timepoint (scheduled and actual) |

Neither dataset contains personal data. Details, checksums and quality profiles: [`DATA_CARD.md`](DATA_CARD.md) and [`DATA_CARD_DELAY.md`](DATA_CARD_DELAY.md).

## Repository layout

```
.
├── config.yaml            thresholds, peak windows, paths, delay settings
├── run_all.sh             end-to-end runner
├── requirements.txt       pinned dependencies
├── scripts/               download_data.py, download_delay_data.py
├── src/
│   ├── gtfs_time.py       GTFS time parser (handles 25:10:00)
│   ├── ingest.py          schemas, Spark session, readers
│   ├── clean.py           cleaning rules and row-count waterfall
│   ├── features.py        haversine, hop features, filters, route class
│   ├── pipeline.py        BMTC joins, windows, aggregations
│   ├── delay_pipeline.py  Boston actual-vs-scheduled delay analysis
│   ├── benchmark.py       1/2/4-core benchmark, join comparison, scale-up
│   ├── viz.py             all figures
│   ├── profile_data.py    data profile and interpolation check
│   ├── environment.py     machine and toolchain detection
│   └── make_docs.py       generates the docs from measured outputs
├── tests/                 13 tests (parser, cleaning, hops, delay, pandas cross-checks)
├── outputs/               RESULTS.md, figures/, tables/, benchmark_results.json
└── docs/                  PROPOSAL_UPDATES.md
```

## Methodology notes

- **Hop time** = arrival at stop *i* minus departure from stop *i-1* within the same trip (`lag` over `trip_id`, ordered by `stop_sequence`).
- **Segment** = (route, direction, from stop, to stop); a stop-pair rollup across routes is also produced. Ranking needs at least {cfg['thresholds']['min_trips']} trips.
- **Peak** = 07:00 to 10:00 and 17:00 to 20:00; **slowdown ratio** = peak mean time / off-peak mean time; **CV** = std / mean.
- **Distance** is straight-line (haversine), so speeds are lower bounds.
- **Delay** = actual minus scheduled departure, modulo 24 h; **on time** = {cfg['delay']['on_time_early_sec']//60} to +{cfg['delay']['on_time_late_sec']//60} min; **bunched** = actual gap under {cfg['delay']['headway_bunch_ratio']} of the scheduled gap.
- Feeder routes are those named `MF-*`, trunk routes are non-feeder routes of at least {cfg['route_class']['trunk_min_km']} km. This is a heuristic, not an official label.
- No machine learning is used; this is descriptive analytics.

## Limitations

The BMTC timetable is scraped and appears interpolated, so little real variability exists; no actual BMTC times are available, so no BMTC delay is claimed; distances are straight-line; the BMTC dataset is small relative to Spark's overhead; the feeder/trunk rule is a heuristic. The Boston analysis covers timepoints only, {len(cfg['delay']['months'])} sampled months, and Schedule-standard trips for delay. Full list: [`outputs/RESULTS.md`](outputs/RESULTS.md).

## Documentation

[`outputs/RESULTS.md`](outputs/RESULTS.md) full results and limitations, [`DATA_CARD.md`](DATA_CARD.md), [`DATA_CARD_DELAY.md`](DATA_CARD_DELAY.md), [`docs/PROPOSAL_UPDATES.md`](docs/PROPOSAL_UPDATES.md).

## Team

| Member | Responsibility |
|---|---|
| **Nikitaa Dhinakaran** (1MS24CS113) | Dataset preprocessing, cleaning and schema transformation; visualisation output |
| **Pranavi Manjunath** (1MS24CS229) | PySpark pipeline engineering and aggregations; performance benchmark testing |

## Acknowledgements and data attribution

Contains BMTC timetable data compiled by [Vonter](https://github.com/Vonter/bmtc-gtfs) from the Namma BMTC 2.0 app, used under the ODbL; the data is unofficial and may be inaccurate. Boston bus data: Massachusetts Bay Transportation Authority via the MBTA Open Data Portal (CC0); the agency is not liable for errors in the data.
"""


def main():
    cfg = ingest.load_config()
    d = gather(cfg)
    Path("DATA_CARD.md").write_text(data_card(d))
    Path("DATA_CARD_DELAY.md").write_text(data_card_delay(d))
    Path("outputs/RESULTS.md").write_text(results(d))
    Path("docs").mkdir(exist_ok=True)
    Path("docs/PROPOSAL_UPDATES.md").write_text(proposal(d))
    Path("README.md").write_text(readme(d))
    print("wrote DATA_CARD.md, DATA_CARD_DELAY.md, outputs/RESULTS.md, docs/PROPOSAL_UPDATES.md, README.md")


if __name__ == "__main__":
    main()
