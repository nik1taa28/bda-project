import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { Pipeline, ScalingEntry, WaterfallRow } from "../lib/data";
import { fmtBytes, fmtInt, fmtNum } from "../lib/format";
import { C } from "../lib/palette";
import { Block, Card, Kpi, Segmented } from "../components/Ui";
import { PipelineDiagram } from "../components/PipelineDiagram";

const axis = { stroke: "var(--grid)" };
const tip = { contentStyle: { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 8, color: "var(--ink)" } };
const xLabel = (v: string) => ({ value: v, position: "insideBottom" as const, offset: -2, fill: "var(--muted)", fontSize: 12 });
const yLabel = (v: string) => ({ value: v, angle: -90, position: "insideLeft" as const, fill: "var(--muted)", fontSize: 12 });

const KIND: Record<WaterfallRow["kind"], { color: string; label: string }> = {
  start: { color: C.blue, label: "start / final" },
  final: { color: C.blue, label: "start / final" },
  quality: { color: C.green, label: "data-quality step" },
  scope: { color: C.orange, label: "scope filter (intended)" },
  structural: { color: C.sky, label: "structural" },
};

type Bench = "pipeline" | "model";

function scalingRows(sc: Record<string, ScalingEntry>) {
  const cores = Object.keys(sc).map(Number).sort((a, b) => a - b);
  return cores.map((c) => ({ cores: c, measured: sc[String(c)].speedup_total, ideal: c, latency: sc[String(c)].total_latency_s_median, efficiency: sc[String(c)].efficiency_total, throughput: sc[String(c)].throughput_rows_per_s ?? null }));
}

export function PipelinePage({ d }: { d: Pipeline }) {
  const [bench, setBench] = useState<Bench>("pipeline");
  const wf = d.waterfall;
  const wfRows = wf.map((r) => ({ ...r, name: r.stage.replace(/^\d+\.\s*/, ""), short: r.stage.replace(/^\d+\.\s*/, "").slice(0, 50), kept: r.rows_out }));

  const B = d.benchmark;
  const env = d.environment;
  const ms = B.model_scaling;
  const sc = bench === "pipeline" ? B.scaling : ms?.scaling;
  const rows = sc ? scalingRows(sc) : [];
  const jc = B.join_comparison;
  const joinRows = (["broadcast", "shuffle"] as const).map((k) => ({
    variant: k === "broadcast" ? "Broadcast join" : "Shuffle (sort-merge) join",
    seconds: jc.variants[k].join_seconds.median,
    shuffle: jc.variants[k].shuffle_write_bytes_per_run_median,
  }));

  const top = rows.length ? rows[rows.length - 1] : null;

  return (
    <>
      <div className="callout">
        <b>One pipeline, from raw CSV to predictions.</b> Explicit schemas, a row-count waterfall, window functions with <code>lag()</code>, aggregations, then Spark MLlib models, all measured on 1, 2 and 4 cores.
      </div>

      <Block title="Pipeline walkthrough" lead="Click a stage to see the file, the functions and how many rows went in and out on real data.">
        <div className="card">
          <PipelineDiagram waterfall={wf} />
        </div>
      </Block>

      <Block title="Data-completeness waterfall" lead="Rows remaining after every cleaning, join and filter step, so each lost row has a reason.">
        <div className="card">
          <ResponsiveContainer width="100%" height={Math.max(260, wfRows.length * 30 + 50)}>
            <BarChart data={wfRows} layout="vertical" margin={{ left: 0, right: 60 }}>
              <CartesianGrid stroke="var(--grid)" horizontal={false} />
              <XAxis type="number" {...axis} tickFormatter={(v) => `${(v / 1e6).toFixed(1)}M`} label={xLabel("Rows remaining")} height={50} />
              <YAxis type="category" dataKey="short" {...axis} width={320} tick={{ fontSize: 12 }} />
              <Tooltip
                {...tip}
                content={({ payload }) => {
                  const r = payload?.[0]?.payload as (WaterfallRow & { name: string }) | undefined;
                  if (!r) return null;
                  return (
                    <div style={{ ...tip.contentStyle, padding: "8px 10px", maxWidth: 320, fontSize: 12.5 }}>
                      <b>{r.name}</b>
                      <div>
                        {fmtInt(r.rows_in)} in, {fmtInt(r.rows_out)} out ({r.pct_retained.toFixed(2)}% kept)
                      </div>
                      <div style={{ color: "var(--muted)" }}>{r.reason}</div>
                    </div>
                  );
                }}
              />
              <Bar dataKey="kept" isAnimationActive={false}>
                {wfRows.map((r) => (
                  <Cell key={r.stage} fill={KIND[r.kind].color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <p className="note">
            Colour key: blue = start and final rows, green = data-quality step, orange = scope filter (intended): Headway-standard trips are analysed through headways, not schedule delay.
          </p>
        </div>
      </Block>

      <Block title="Performance benchmarks" lead="Each core count runs in its own process, after one discarded warm-up, and the median of the timed runs is reported. Speedup S = T1 / TN and efficiency = S / N.">
        <div className="controls">
          <Segmented<Bench> label="Benchmark" value={bench} onChange={setBench} options={[{ value: "pipeline", label: "Analysis pipeline" }, ...(ms ? [{ value: "model" as Bench, label: "Model training" }] : [])]} />
        </div>

        {sc && top && (
          <>
            <div className="kpis">
              <Kpi label={`Speedup on ${top.cores} cores`} value={`${fmtNum(top.measured, 2)}x`} hint={`ideal would be ${top.cores}x`} />
              <Kpi label={`Efficiency on ${top.cores} cores`} value={fmtNum(top.efficiency, 2)} hint="speedup divided by cores" accent="var(--green)" />
              <Kpi label="Total time, 1 core" value={`${fmtNum(rows[0].latency, 1)} s`} hint={`${top.cores} cores: ${fmtNum(top.latency, 1)} s`} accent="var(--orange)" />
              <Kpi
                label={bench === "pipeline" ? "Events processed" : "Training rows"}
                value={fmtInt(bench === "pipeline" ? B.cleaned_event_rows : ms?.train_rows_used)}
                hint={bench === "pipeline" ? `${B.config.timed_runs} timed runs, ${B.config.warmup_runs} warm-up` : `${ms?.timed_runs} timed runs, ${ms?.warmup_runs} warm-up`}
                accent="var(--vermillion)"
              />
            </div>
            <div className="grid3">
              <Card title="Speedup" sub="Measured vs ideal linear">
                <ResponsiveContainer width="100%" height={240}>
                  <LineChart data={rows} margin={{ left: 0, right: 14, top: 8, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="cores" type="number" domain={[1, rows[rows.length - 1].cores]} ticks={rows.map((r) => r.cores)} {...axis} label={xLabel("Spark cores (local[N])")} height={50} />
                    <YAxis {...axis} width={40} domain={[0, "auto"]} />
                    <Tooltip {...tip} formatter={(v: number, n) => [`${v.toFixed(2)}x`, n]} />
                    <Legend />
                    <Line isAnimationActive={false} name="Ideal" dataKey="ideal" stroke={C.grey} strokeDasharray="5 4" strokeWidth={1.5} dot={false} />
                    <Line name="Measured" dataKey="measured" stroke={C.blue} strokeWidth={2.5} dot={{ r: 5 }} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </Card>
              <Card title="Latency" sub="Median total seconds">
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={rows} margin={{ left: 0, right: 14, top: 8, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="cores" {...axis} label={xLabel("Spark cores")} height={50} />
                    <YAxis {...axis} width={44} label={yLabel("seconds")} />
                    <Tooltip {...tip} formatter={(v: number) => [`${v.toFixed(1)} s`, "Median time"]} />
                    <Bar dataKey="latency" fill={C.blue} radius={[4, 4, 0, 0]} maxBarSize={60} isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
              <Card title="Efficiency" sub="Speedup divided by cores">
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={rows} margin={{ left: 0, right: 14, top: 8, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="cores" {...axis} label={xLabel("Spark cores")} height={50} />
                    <YAxis {...axis} width={40} domain={[0, 1]} ticks={[0, 0.25, 0.5, 0.75, 1]} />
                    <Tooltip {...tip} formatter={(v: number) => [v.toFixed(2), "Efficiency"]} />
                    <Bar dataKey="efficiency" fill={C.green} radius={[4, 4, 0, 0]} maxBarSize={60} isAnimationActive={false} />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            </div>
            <p className="mini" style={{ marginTop: 10 }}>
              {bench === "pipeline"
                ? "Millions of real events: ingest, clean, window and aggregate. The cores are local threads on one machine, not a cluster, so speedup is sub-linear."
                : `Feature preparation plus training of a GBT regressor and classifier (${ms?.gbt.gbt_max_iter} rounds, depth ${ms?.gbt.gbt_max_depth}). Tree training has serial steps, so speedup is sub-linear.`}
            </p>
          </>
        )}
      </Block>

      <Block title="Broadcast join vs shuffle join" lead="The model joins small route and route+timepoint history tables onto millions of events. A broadcast join copies the small tables to every worker so the big table never moves; a shuffle (sort-merge) join redistributes both tables.">
        <div className="grid2">
          <Card title="Join stage time" sub={`Median seconds on ${jc.cores} cores`}>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={joinRows} layout="vertical" margin={{ left: 0, right: 40, bottom: 14 }}>
                <CartesianGrid stroke="var(--grid)" horizontal={false} />
                <XAxis type="number" {...axis} domain={[0, (m: number) => Math.ceil(m)]} allowDecimals={false} label={xLabel("seconds")} height={48} />
                <YAxis type="category" dataKey="variant" {...axis} width={150} />
                <Tooltip {...tip} formatter={(v: number) => [`${v.toFixed(2)} s`, "Median time"]} />
                <Bar dataKey="seconds" radius={[0, 4, 4, 0]} isAnimationActive={false}>
                  <Cell fill={C.green} />
                  <Cell fill={C.vermillion} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </Card>
          <Card title="Data written to shuffle" sub="Per run, from the Spark event log">
            <dl className="kv" style={{ marginTop: 6 }}>
              {joinRows.map((r) => (
                <div key={r.variant} style={{ display: "contents" }}>
                  <dt>{r.variant}</dt>
                  <dd>{typeof r.shuffle === "number" ? fmtBytes(r.shuffle) : "not measured"}</dd>
                </div>
              ))}
            </dl>
            <p className="note">Shuffle is forced by setting autoBroadcastJoinThreshold to -1. Adaptive execution is off so Spark cannot switch strategy during the run.</p>
          </Card>
        </div>
      </Block>

      <Block title="Correctness and environment">
        <div className="grid2">
          <Card title="Spark vs pandas cross-checks" sub="Spark results recomputed independently and compared">
            <dl className="kv">
              <dt>Delay pipeline</dt>
              <dd>
                {d.correctness_delay?.passed ? <span className="tag ok">passed</span> : <span className="tag no">not passed</span>} {fmtInt(d.correctness_delay?.events_compared)} events, {d.correctness_delay?.routes_checked.length} routes (recomputed from the raw CSV)
              </dd>
              <dt>Max difference</dt>
              <dd>{d.correctness_delay ? d.correctness_delay.max_abs_diff_mean_std_on_time.toExponential(1) : "n/a"}</dd>
            </dl>
            <p className="note">A sample of routes only; both sides were written by the same team. The model tests (tests/test_delay_model.py) check that no feature uses the current or later events of a trip, that train and test days do not overlap, and that history features use training days only.</p>
          </Card>
          <Card title="Test machine" sub="Detected by src/environment.py">
            <dl className="kv">
              <dt>CPU</dt>
              <dd>{env.cpu}</dd>
              <dt>Cores</dt>
              <dd>
                {env.logical_cores} ({env.performance_cores} performance + {env.efficiency_cores} efficiency)
              </dd>
              <dt>Memory</dt>
              <dd>{env.ram_gb} GB</dd>
              <dt>Software</dt>
              <dd>
                PySpark {env.pyspark}, Python {env.python}
              </dd>
              <dt>Java</dt>
              <dd>{env.java}</dd>
              <dt>Spark config</dt>
              <dd>shuffle.partitions = {B.config.shuffle_partitions}, adaptive off</dd>
            </dl>
          </Card>
        </div>
      </Block>
    </>
  );
}
