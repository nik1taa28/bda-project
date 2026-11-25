import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis } from "recharts";
import type { Boston } from "../lib/data";
import { fmtInt, fmtMin, fmtNum, fmtPct, histSeries, matches, median } from "../lib/format";
import { C } from "../lib/palette";
import { Block, Card, Kpi, ScopeBanner, Segmented, Slider } from "../components/Ui";
import { DataTable, type Column } from "../components/DataTable";

const axis = { stroke: "var(--grid)" };
const tip = { contentStyle: { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 8, color: "var(--ink)" } };
const xLabel = (v: string) => ({ value: v, position: "insideBottom" as const, offset: -2, fill: "var(--muted)", fontSize: 12 });
const yLabel = (v: string) => ({ value: v, angle: -90, position: "insideLeft" as const, fill: "var(--muted)", fontSize: 12 });

type Metric = "mean_min" | "on_time" | "late" | "early";

export function BostonPage({ d }: { d: Boston }) {
  const o = d.overall;
  const st = d.settings;
  const [metric, setMetric] = useState<Metric>("on_time");
  const [query, setQuery] = useState("");
  const [minEvents, setMinEvents] = useState(st.min_events);

  const hist = histSeries(d.delay_hist_min).map((r) => ({ ...r, inWindow: r.x >= st.on_time_early_sec / 60 && r.x <= st.on_time_late_sec / 60 }));

  const hourRows = d.by_hour.map((h) => ({
    hour: h.hour_of_day,
    mean_min: h.mean_delay_sec / 60,
    on_time: h.on_time_rate * 100,
    late: h.late_rate * 100,
    early: h.early_rate * 100,
    n: h.n,
  }));
  const metricInfo: Record<Metric, { label: string; unit: string; color: string }> = {
    on_time: { label: "On-time share", unit: "%", color: C.green },
    late: { label: "Late share (more than 5 min)", unit: "%", color: C.vermillion },
    early: { label: "Early share (more than 1 min)", unit: "%", color: C.sky },
    mean_min: { label: "Mean delay", unit: "min", color: C.blue },
  };
  const mi = metricInfo[metric];

  const dayPeriod = ["OFF_PEAK", "PEAK"].map((p) => ({
    period: p === "PEAK" ? "Peak (07-10h, 17-20h)" : "Off-peak",
    weekday: (d.by_daytype_period.find((r) => r.day_type === "weekday" && r.period === p)?.mean_delay_sec ?? 0) / 60,
    weekend: (d.by_daytype_period.find((r) => r.day_type === "weekend" && r.period === p)?.mean_delay_sec ?? 0) / 60,
  }));
  const orderRows = d.by_timepoint_order.map((r) => ({ order: r.time_point_order, mean_min: r.mean_delay_sec / 60, n: r.n }));
  const pointRows = [...d.by_point_type].sort((a, b) => ['Startpoint', 'Midpoint', 'Endpoint'].indexOf(a.point_type) - ['Startpoint', 'Midpoint', 'Endpoint'].indexOf(b.point_type)).map((r) => ({ type: r.point_type, mean_min: r.mean_delay_sec / 60, on_time: r.on_time_rate * 100 }));

  const routes = useMemo(() => d.by_route.filter((r) => r.n >= minEvents && matches(r.route_id, query)), [d, minEvents, query]);
  const topRoutes = useMemo(() => [...routes].sort((a, b) => b.mean_delay_sec - a.mean_delay_sec).slice(0, 12).map((r) => ({ route: r.route_id, mean_min: r.mean_delay_sec / 60, n: r.n })), [routes]);
  type RouteRow = (typeof d.by_route)[number];
  const routeCols: Column<RouteRow>[] = [
    { key: "route_id", label: "Route" },
    { key: "n", label: "Events", num: true, render: (r) => fmtInt(r.n) },
    { key: "mean_delay_sec", label: "Mean delay", num: true, render: (r) => fmtMin(r.mean_delay_sec) },
    { key: "p90_delay_sec", label: "90th pct", num: true, render: (r) => fmtMin(r.p90_delay_sec) },
    { key: "on_time_rate", label: "On time", num: true, render: (r) => fmtPct(r.on_time_rate) },
    { key: "late_rate", label: "Late", num: true, render: (r) => fmtPct(r.late_rate) },
  ];

  const seg = d.seg_slowdown;
  const medSched = median(seg.map((s) => s.sched_slowdown_ratio));
  const medAct = median(seg.map((s) => s.actual_slowdown_ratio));

  const hw = d.headway_by_hour.map((h) => ({ hour: h.hour_of_day, bunched: h.bunched_rate * 100, gap: h.gap_rate * 100 }));
  const hwRoutes = [...d.headway_by_route].filter((r) => r.n >= st.min_events).sort((a, b) => b.bunched_rate - a.bunched_rate).slice(0, 12).map((r) => ({ route: r.route_id, bunched: r.bunched_rate * 100, n: r.n }));
  const hop = d.hop_by_hour.map((h) => ({ hour: h.hour_of_day, scheduled: h.mean_sched_hop, actual: h.mean_act_hop }));

  return (
    <>
      <ScopeBanner kind="boston" title="Boston · MBTA">
        <b>Real delay.</b> Boston's public bus data records both the scheduled and the actual time of every bus at every timepoint, so delay (actual minus scheduled) can be measured directly. Data: 2024, {st.months.length} sampled months.
      </ScopeBanner>

      <div className="kpis">
        <Kpi label="Mean departure delay" value={fmtMin(o.mean_delay_sec)} hint={`median ${fmtMin(o.median_delay_sec)}, 90th percentile ${fmtMin(o.p90_delay_sec)}`} accent="var(--boston)" />
        <Kpi label="On time" value={fmtPct(o.on_time_rate)} hint={`between ${st.on_time_early_sec / 60} and +${st.on_time_late_sec / 60} min of schedule`} accent="var(--green)" />
        <Kpi label="Late / early" value={`${fmtPct(o.late_rate)} / ${fmtPct(o.early_rate)}`} hint={`${fmtInt(o.n)} events on Schedule-standard trips`} accent="var(--vermillion)" />
        <Kpi label="Bunched gaps" value={fmtPct(d.headway_overall.bunched_rate)} hint={`under ${st.headway_bunch_ratio}x the scheduled gap; large gaps ${fmtPct(d.headway_overall.gap_rate)}`} accent="var(--orange)" />
      </div>

      <Block title="How late are buses?" lead="Delay = actual minus scheduled departure at a timepoint. Positive means late.">
        <div className="grid2">
          <Card title="Distribution of delay" sub="Share of events in each 30-second bin (2% random sample, clipped to -10..30 min)">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={hist} barCategoryGap={0} margin={{ left: 0, right: 12, top: 6, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="x" type="number" domain={[-10, 30]} ticks={[-10, -5, 0, 5, 10, 15, 20, 25, 30]} {...axis} label={xLabel("Delay (minutes)")} height={48} />
                <YAxis {...axis} width={48} tickFormatter={(v) => `${(v * 100).toFixed(1)}%`} />
                <Tooltip {...tip} formatter={(v: number) => [`${(v * 100).toFixed(2)}%`, "Share of events"]} labelFormatter={(x) => `${x} min`} />
                <Bar dataKey="y" isAnimationActive={false}>
                  {hist.map((r) => (
                    <Cell key={r.x} fill={r.inWindow ? C.green : C.blue} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
            <p className="note">Green bars are the on-time window. Most events are slightly late, with a long tail of large delays.</p>
          </Card>
          <Card title="By hour of day" sub="Scheduled hour of the departure">
            <div className="controls" style={{ marginBottom: 6 }}>
              <Segmented<Metric> label="Metric" value={metric} onChange={setMetric} options={[{ value: "on_time", label: "On time" }, { value: "late", label: "Late" }, { value: "early", label: "Early" }, { value: "mean_min", label: "Mean delay" }]} />
            </div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={hourRows} margin={{ left: 0, right: 12, top: 6, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="hour" {...axis} label={xLabel("Hour of day")} height={48} />
                <YAxis {...axis} width={46} label={yLabel(mi.unit)} />
                <Tooltip {...tip} formatter={(v: number, _n, p) => [`${v.toFixed(1)} ${mi.unit} (n=${fmtInt((p.payload as { n: number }).n)})`, mi.label]} labelFormatter={(h) => `${h}:00`} />
                <Line dataKey={metric} stroke={mi.color} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </Card>
          <Card title="Peak vs off-peak, weekday vs weekend" sub="Mean delay in minutes">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={dayPeriod} margin={{ left: 0, right: 12, top: 6, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="period" {...axis} />
                <YAxis {...axis} width={44} label={yLabel("minutes")} />
                <Tooltip {...tip} formatter={(v: number, n) => [`${v.toFixed(2)} min`, n]} />
                <Legend />
                <Bar isAnimationActive={false} dataKey="weekday" name="Weekday" fill={C.blue} radius={[4, 4, 0, 0]} maxBarSize={60} />
                <Bar isAnimationActive={false} dataKey="weekend" name="Weekend" fill={C.orange} radius={[4, 4, 0, 0]} maxBarSize={60} />
              </BarChart>
            </ResponsiveContainer>
            <p className="note">Delay itself barely differs between peak and off-peak.</p>
          </Card>
          <Card title="Delay by position in the trip" sub="Mean delay at the 1st, 2nd, ... timepoint">
            <ResponsiveContainer width="100%" height={240}>
              <LineChart data={orderRows} margin={{ left: 0, right: 12, top: 6, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="order" {...axis} label={xLabel("Timepoint order (1 = start of trip)")} height={48} />
                <YAxis {...axis} width={44} label={yLabel("minutes")} />
                <Tooltip {...tip} formatter={(v: number, _n, p) => [`${v.toFixed(2)} min (n=${fmtInt((p.payload as { n: number }).n)})`, "Mean delay"]} labelFormatter={(x) => `Timepoint ${x}`} />
                <Line dataKey="mean_min" stroke={C.blue} strokeWidth={2} dot={{ r: 3 }} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
            <p className="note">Not a steady build-up: it rises, then falls, and the highest positions have far fewer events (see tooltip).</p>
          </Card>
        </div>
        <div className="grid2" style={{ marginTop: 14 }}>
          <Card title="By stop type" sub="Start, middle and end timepoints">
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={pointRows} margin={{ left: 0, right: 12 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="type" {...axis} />
                <YAxis {...axis} width={44} label={yLabel("minutes")} />
                <Tooltip {...tip} formatter={(v: number, _n, p) => [`${v.toFixed(2)} min (on time ${(p.payload as { on_time: number }).on_time.toFixed(1)}%)`, "Mean delay"]} />
                <Bar isAnimationActive={false} dataKey="mean_min" fill={C.blue} radius={[4, 4, 0, 0]} maxBarSize={70} />
              </BarChart>
            </ResponsiveContainer>
          </Card>
          <Card title="Actual vs scheduled run time by hour" sub="Mean seconds between consecutive timepoints">
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={hop} margin={{ left: 0, right: 12, top: 6, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" vertical={false} />
                <XAxis dataKey="hour" {...axis} label={xLabel("Hour of day")} height={48} />
                <YAxis {...axis} width={44} domain={["auto", "auto"]} />
                <Tooltip {...tip} formatter={(v: number, n) => [`${v.toFixed(0)} s`, n]} labelFormatter={(h) => `${h}:00`} />
                <Legend />
                <Line name="Scheduled" dataKey="scheduled" stroke={C.grey} strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line name="Actual" dataKey="actual" stroke={C.vermillion} strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
            <p className="note">On average buses add {fmtNum(d.hop_overall.mean_added_sec, 1)} s per hop ({fmtNum(d.hop_overall.mean_sched_hop, 0)} s scheduled vs {fmtNum(d.hop_overall.mean_act_hop, 0)} s actual).</p>
          </Card>
        </div>
      </Block>

      <Block title="Route explorer" lead="Delay by route. Click a column header to sort; routes need enough events to be listed.">
        <div className="controls">
          <label className="field">
            Search
            <input type="search" placeholder="route id" value={query} onChange={(e) => setQuery(e.target.value)} />
          </label>
          <Slider label="Min events" min={st.min_events} max={20000} step={100} value={minEvents} onChange={setMinEvents} />
        </div>
        <div className="grid2">
          <div className="card">
            <DataTable rows={routes} columns={routeCols} defaultSort="mean_delay_sec" />
          </div>
          <Card title="Most delayed routes" sub="Top 12 by mean delay among the routes listed">
            <ResponsiveContainer width="100%" height={Math.max(220, topRoutes.length * 26 + 40)}>
              <BarChart data={topRoutes} layout="vertical" margin={{ left: 0, right: 40, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" horizontal={false} />
                <XAxis type="number" {...axis} label={xLabel("Mean delay (minutes)")} height={48} />
                <YAxis type="category" dataKey="route" {...axis} width={48} />
                <Tooltip {...tip} formatter={(v: number, _n, p) => [`${v.toFixed(2)} min (n=${fmtInt((p.payload as { n: number }).n)})`, "Mean delay"]} />
                <Bar isAnimationActive={false} dataKey="mean_min" fill={C.vermillion} radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </Card>
        </div>
      </Block>

      <Block title="Does the timetable plan the peak slowdown?" lead="Each dot is a segment between two consecutive timepoints. Dots near the dashed line mean the timetable already plans the slowdown that really happens.">
        <div className="grid2">
          <Card title="Scheduled vs actual slowdown ratio" sub={`${fmtInt(seg.length)} segments with at least ${st.min_events} peak and off-peak events`}>
            <ResponsiveContainer width="100%" height={340}>
              <ScatterChart margin={{ left: 0, right: 16, top: 8 }}>
                <CartesianGrid stroke="var(--grid)" />
                <XAxis type="number" dataKey="sched_slowdown_ratio" name="Scheduled" domain={[0.6, 1.8]} allowDataOverflow ticks={[0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8]} {...axis} label={xLabel("Scheduled ratio (peak / off-peak run time)")} height={52} />
                <YAxis type="number" dataKey="actual_slowdown_ratio" name="Actual" domain={[0.6, 1.8]} allowDataOverflow ticks={[0.6, 0.8, 1, 1.2, 1.4, 1.6, 1.8]} {...axis} width={50} label={yLabel("Actual ratio")} />
                <ZAxis range={[14, 14]} />
                <Tooltip {...tip} cursor={{ strokeDasharray: "3 3" }} formatter={(v: number) => v.toFixed(3)} />
                <ReferenceLine segment={[{ x: 0.6, y: 0.6 }, { x: 1.8, y: 1.8 }]} stroke={C.grey} strokeDasharray="5 4" />
                <Scatter data={seg} fill={C.blue} fillOpacity={0.45} isAnimationActive={false} />
              </ScatterChart>
            </ResponsiveContainer>
            <p className="note">
              Axes are clipped to 0.6 to 1.8, so a few outlier segments are not drawn. Median scheduled ratio {fmtNum(medSched, 3)} vs median actual ratio {fmtNum(medAct, 3)}. Many Boston segments already have a planned peak slowdown in the timetable; dots far from the line show where reality differs from the plan.
            </p>
          </Card>
          <div style={{ display: "grid", gap: 14, alignContent: "start" }}>
            <Card title="Headway: bunching and large gaps by hour" sub="High-frequency routes only; bunched = actual gap under half the scheduled gap">
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={hw} margin={{ left: 0, right: 12, top: 6, bottom: 14 }}>
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis dataKey="hour" {...axis} label={xLabel("Hour of day")} height={48} />
                  <YAxis {...axis} width={44} tickFormatter={(v) => `${v}%`} />
                  <Tooltip {...tip} formatter={(v: number, n) => [`${v.toFixed(1)}%`, n]} labelFormatter={(h) => `${h}:00`} />
                  <Legend />
                  <Line name="Bunched" dataKey="bunched" stroke={C.orange} strokeWidth={2} dot={false} isAnimationActive={false} />
                  <Line name="Large gap (> 1.5x)" dataKey="gap" stroke={C.blue} strokeWidth={2} dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </Card>
            <Card title="Routes with the most bunching" sub={`Top 12, at least ${st.min_events} headway events`}>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={hwRoutes} layout="vertical" margin={{ left: 0, right: 40, bottom: 14 }}>
                  <CartesianGrid stroke="var(--grid)" horizontal={false} />
                  <XAxis type="number" {...axis} tickFormatter={(v) => `${v}%`} />
                  <YAxis type="category" dataKey="route" {...axis} width={48} />
                  <Tooltip {...tip} formatter={(v: number, _n, p) => [`${v.toFixed(1)}% (n=${fmtInt((p.payload as { n: number }).n)})`, "Bunched share"]} />
                  <Bar isAnimationActive={false} dataKey="bunched" fill={C.orange} radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          </div>
        </div>
      </Block>

      <div className="callout warm">
        <b>Limits of this analysis.</b> Boston only; timepoints (not every stop); {st.months.length} sampled months of 2024; delay is judged on Schedule-standard trips only, and Headway-standard trips are analysed through headways.
      </div>
    </>
  );
}
