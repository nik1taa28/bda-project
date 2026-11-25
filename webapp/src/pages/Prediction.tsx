import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis } from "recharts";
import type { ClfMetrics, Model, RegMetrics } from "../lib/data";
import { fmtInt, fmtNum, fmtPct } from "../lib/format";
import { C } from "../lib/palette";
import { Block, Card, Kpi, ScopeBanner, Segmented } from "../components/Ui";

const axis = { stroke: "var(--grid)" };
const tip = { contentStyle: { background: "var(--surface)", border: "1px solid var(--line)", borderRadius: 8, color: "var(--ink)" } };
const xLabel = (v: string) => ({ value: v, position: "insideBottom" as const, offset: -2, fill: "var(--muted)", fontSize: 12 });
const yLabel = (v: string) => ({ value: v, angle: -90, position: "insideLeft" as const, fill: "var(--muted)", fontSize: 12 });

const FULL = "GBT (full features)";
const NOPREV = "GBT without previous delay";
const MEAN = "baseline: train mean";
const CARRY = "baseline: previous delay carried forward";
const NEVER = "baseline: never late";

const SHORT: Record<string, string> = {
  [MEAN]: "Train mean",
  [CARRY]: "Previous delay carried",
  [NOPREV]: "GBT, no previous delay",
  [FULL]: "GBT (full)",
  [NEVER]: "Never late",
};
const COLOR: Record<string, string> = { [MEAN]: C.grey, [CARRY]: C.sky, [NOPREV]: C.orange, [FULL]: C.blue, [NEVER]: C.grey };

type Task = "regression" | "classification";

export function PredictionPage({ d }: { d: Model }) {
  const r = d.results;
  const [task, setTask] = useState<Task>("regression");
  const reg = r.regression;
  const clf = r.classification;
  const g = reg[FULL];
  const bestBaseMae = Math.min(reg[MEAN].mae_min, reg[CARRY].mae_min);
  const gain = (100 * (bestBaseMae - g.mae_min)) / bestBaseMae;
  const cg = clf[FULL];

  const regOrder = [MEAN, CARRY, NOPREV, FULL];
  const clfOrder = [NEVER, CARRY, NOPREV, FULL];
  const maeRows = regOrder.map((k) => ({ name: SHORT[k], key: k, mae: reg[k].mae_min }));
  const hour = d.error_by_hour.map((h) => ({ hour: h.hour_of_day, mean: h.mean_baseline_mae_min, carry: h.carry_baseline_mae_min, gbt: h.gbt_mae_min, n: h.n }));
  const cal = d.regression_calibration.filter((c) => c.n >= 100);
  const ccal = d.classification_calibration.filter((c) => c.n >= 100);
  const scatter = d.prediction_sample.map(([a, p]) => ({ actual: Math.max(-5, Math.min(25, a)), pred: Math.max(-5, Math.min(25, p)) }));
  const imp = d.feature_importance.filter((f) => f.model === FULL && f.task === task).sort((a, b) => b.importance - a.importance);
  const cm = [
    [cg.tn, cg.fp],
    [cg.fn, cg.tp],
  ];
  const lateMin = r.late_threshold_sec / 60;

  return (
    <>
      <ScopeBanner kind="boston" title="Boston · MBTA prediction">
        <b>Predicting delay with Spark MLlib.</b> At the moment a bus leaves its previous timepoint, the models predict its delay at the next one. They were trained on earlier days and are scored only on later days they never saw (the last {fmtNum(r.test_fraction * 100, 0)}% of the days of every month).
      </ScopeBanner>

      <div className="kpis">
        <Kpi label="Mean error, GBT" value={`${fmtNum(g.mae_min, 2)} min`} hint={`${fmtNum(bestBaseMae, 2)} min for the best simple baseline (${fmtNum(gain, 0)}% lower)`} accent="var(--blue)" />
        <Kpi label="R² (delay in minutes)" value={fmtNum(g.r2, 2)} hint={`without the previous delay: ${fmtNum(reg[NOPREV].r2, 2)}`} accent="var(--green)" />
        <Kpi label={`AUC, late > ${lateMin} min`} value={fmtNum(cg.auc, 3)} hint={`recall ${fmtPct(cg.recall)}, precision ${fmtPct(cg.precision)}`} accent="var(--vermillion)" />
        <Kpi label="Test events" value={fmtInt(r.test_rows)} hint={`${r.split.test.days} held-out days; trained on ${fmtInt(r.train_rows_used)} events`} accent="var(--orange)" />
      </div>

      <Block title="How the models were tested" lead="A random split would let the model see the same days it is tested on. A temporal split avoids that.">
        <div className="grid2">
          <Card title="Train and test days">
            <dl className="kv">
              <dt>Train</dt>
              <dd>
                {fmtInt(r.split.train.rows)} events, {r.split.train.days} days ({r.split.train.first_date} to {r.split.train.last_date}); fitted on a seeded sample of {fmtInt(r.train_rows_used)}
              </dd>
              <dt>Test</dt>
              <dd>
                {fmtInt(r.split.test.rows)} events, {r.split.test.days} days ({r.split.test.first_date} to {r.split.test.last_date})
              </dd>
              <dt>Late share</dt>
              <dd>
                train {fmtPct(r.split.train.late_rate)}, test {fmtPct(r.split.test.late_rate)}
              </dd>
              <dt>Models</dt>
              <dd>
                Gradient-boosted trees (GBTRegressor, GBTClassifier), {r.gbt.gbt_max_iter} rounds, depth {r.gbt.gbt_max_depth}
              </dd>
            </dl>
          </Card>
          <Card title="Features" sub="All known before the event happens">
            <p className="mini">
              {r.features_full
                .map((f) => f.replace(/_idx$/, "").replace(/_/g, " "))
                .join(", ")}
              . The route averages (<code>route_hist</code>, <code>rtp_hist</code>) are computed on training days only. <code>prev delay min</code> is the delay at the previous timepoint of the same trip.
            </p>
            <p className="note">A second model leaves out the previous-delay features, to show how much of the skill comes from them.</p>
          </Card>
        </div>
      </Block>

      <Block title="Results" lead="Every model is compared with simple baselines, so the numbers have something to be measured against.">
        <div className="controls">
          <Segmented<Task> label="Task" value={task} onChange={setTask} options={[{ value: "regression", label: "Delay in minutes" }, { value: "classification", label: `Late (> ${lateMin} min)` }]} />
        </div>

        {task === "regression" ? (
          <>
            <div className="grid2">
              <Card title="Model comparison" sub="Mean absolute error on test days, lower is better">
                <ResponsiveContainer width="100%" height={250}>
                  <BarChart data={maeRows} margin={{ left: 0, right: 12, top: 18, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="name" {...axis} interval={0} tick={{ fontSize: 11 }} height={48} />
                    <YAxis {...axis} width={44} label={yLabel("minutes")} />
                    <Tooltip {...tip} formatter={(v: number) => [`${v.toFixed(2)} min`, "MAE"]} />
                    <Bar dataKey="mae" isAnimationActive={false} radius={[4, 4, 0, 0]} maxBarSize={64} label={{ position: "top", fill: "var(--ink)", fontSize: 11, formatter: (v: number) => v.toFixed(2) }}>
                      {maeRows.map((m) => (
                        <Cell key={m.key} fill={COLOR[m.key]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </Card>
              <Card title="Metrics" sub="Held-out test days">
                <MetricTable rows={regOrder.map((k) => ({ name: SHORT[k], key: k, vals: [fmtNum(reg[k].rmse_min, 2), fmtNum(reg[k].mae_min, 2), fmtNum(reg[k].r2, 3)] }))} heads={["RMSE (min)", "MAE (min)", "R²"]} />
                <p className="note">RMSE punishes big misses more than MAE does, so it is larger because a few events are very late.</p>
              </Card>
              <Card title="Calibration" sub="Mean actual delay for each predicted delay (1-minute bins)">
                <ResponsiveContainer width="100%" height={260}>
                  <LineChart data={cal} margin={{ left: 0, right: 14, top: 8, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis type="number" dataKey="mean_predicted_min" domain={[-3, 12]} {...axis} label={xLabel("Predicted delay (minutes)")} height={50} />
                    <YAxis type="number" domain={[-3, 12]} {...axis} width={44} label={yLabel("actual (minutes)")} />
                    <Tooltip {...tip} formatter={(v: number, n) => [`${v.toFixed(2)} min`, n]} labelFormatter={(x) => `predicted ${Number(x).toFixed(1)} min`} />
                    <ReferenceLine segment={[{ x: -3, y: -3 }, { x: 12, y: 12 }]} stroke={C.grey} strokeDasharray="5 4" />
                    <Line name="Mean actual" dataKey="mean_actual_min" stroke={C.blue} strokeWidth={2.5} dot={{ r: 4 }} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
                <p className="note">On the dashed line the model would be perfectly calibrated.</p>
              </Card>
              <Card title="Error by hour of day" sub="Mean absolute error, test days">
                <ResponsiveContainer width="100%" height={260}>
                  <LineChart data={hour} margin={{ left: 0, right: 14, top: 8, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" vertical={false} />
                    <XAxis dataKey="hour" {...axis} label={xLabel("Scheduled hour of day")} height={50} />
                    <YAxis {...axis} width={44} label={yLabel("minutes")} />
                    <Tooltip {...tip} formatter={(v: number, n) => [`${v.toFixed(2)} min`, n]} labelFormatter={(h) => `${h}:00`} />
                    <Legend />
                    <Line name="Train mean" dataKey="mean" stroke={C.grey} strokeWidth={2} dot={false} isAnimationActive={false} />
                    <Line name="Previous delay carried" dataKey="carry" stroke={C.sky} strokeWidth={2} dot={false} isAnimationActive={false} />
                    <Line name="GBT (full)" dataKey="gbt" stroke={C.blue} strokeWidth={2.5} dot={false} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </Card>
            </div>
            <div style={{ marginTop: 14 }}>
              <Card title="Predicted vs actual delay" sub={`${fmtInt(scatter.length)} random test events, clipped to -5..25 min`}>
                <ResponsiveContainer width="100%" height={320}>
                  <ScatterChart margin={{ left: 0, right: 16, top: 8, bottom: 14 }}>
                    <CartesianGrid stroke="var(--grid)" />
                    <XAxis type="number" dataKey="pred" domain={[-5, 25]} {...axis} label={xLabel("Predicted delay (minutes)")} height={50} />
                    <YAxis type="number" dataKey="actual" domain={[-5, 25]} {...axis} width={50} label={yLabel("Actual delay (minutes)")} />
                    <ZAxis range={[14, 14]} />
                    <Tooltip {...tip} cursor={{ strokeDasharray: "3 3" }} formatter={(v: number) => `${v.toFixed(1)} min`} />
                    <ReferenceLine segment={[{ x: -5, y: -5 }, { x: 25, y: 25 }]} stroke={C.grey} strokeDasharray="5 4" />
                    <Scatter data={scatter} fill={C.blue} fillOpacity={0.35} isAnimationActive={false} />
                  </ScatterChart>
                </ResponsiveContainer>
                <p className="note">Points near the dashed line are good predictions. The spread around it shows the error that remains.</p>
              </Card>
            </div>
          </>
        ) : (
          <div className="grid2">
            <Card title="Metrics" sub={`Late = more than ${lateMin} min; held-out test days`}>
              <MetricTable
                rows={clfOrder.map((k) => ({ name: SHORT[k], key: k, vals: clfVals(clf[k]) }))}
                heads={["Accuracy", "Precision", "Recall", "F1", "AUC"]}
              />
              <p className="note">The late share on test days is {fmtPct(r.split.test.late_rate)}, so "never late" is already {fmtPct(clf[NEVER].accuracy)} accurate. Look at recall, F1 and AUC instead of accuracy.</p>
            </Card>
            <Card title="Confusion matrix" sub="GBT (full), rows = what really happened">
              <div className="cm">
                <div />
                <div className="h">Predicted on time</div>
                <div className="h">Predicted late</div>
                {["Actually on time", "Actually late"].map((lab, i) => (
                  <div key={lab} style={{ display: "contents" }}>
                    <div className="h">{lab}</div>
                    {cm[i].map((v, j) => {
                      const share = v / (cm[i][0] + cm[i][1]);
                      const good = i === j;
                      return (
                        <div key={j} className="cell" style={{ background: `color-mix(in srgb, ${good ? C.blue : C.vermillion} ${Math.round(share * 70) + 8}%, var(--surface))` }}>
                          <b>{fmtInt(v)}</b>
                          <span>{fmtPct(share, 0)} of row</span>
                        </div>
                      );
                    })}
                  </div>
                ))}
              </div>
            </Card>
            <Card title="Reliability" sub="Actual late share for each predicted probability (10 bins)">
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={ccal} margin={{ left: 0, right: 14, top: 8, bottom: 14 }}>
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis type="number" dataKey="mean_predicted_prob" domain={[0, 1]} {...axis} tickFormatter={(v) => `${Math.round(v * 100)}%`} label={xLabel("Predicted probability of being late")} height={50} />
                  <YAxis type="number" domain={[0, 1]} {...axis} width={50} tickFormatter={(v) => `${Math.round(v * 100)}%`} label={yLabel("actual late share")} />
                  <Tooltip {...tip} formatter={(v: number) => [`${(v * 100).toFixed(1)}%`, "Actual late share"]} labelFormatter={(x) => `predicted ${(Number(x) * 100).toFixed(0)}%`} />
                  <ReferenceLine segment={[{ x: 0, y: 0 }, { x: 1, y: 1 }]} stroke={C.grey} strokeDasharray="5 4" />
                  <Line name="Actual late share" dataKey="actual_late_rate" stroke={C.vermillion} strokeWidth={2.5} dot={{ r: 4 }} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
              <p className="note">On the dashed line the probabilities would be perfectly calibrated.</p>
            </Card>
            <Card title="Model comparison" sub="F1 score on test days, higher is better">
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={clfOrder.map((k) => ({ name: SHORT[k], key: k, f1: clf[k].f1 }))} margin={{ left: 0, right: 12, top: 18, bottom: 14 }}>
                  <CartesianGrid stroke="var(--grid)" vertical={false} />
                  <XAxis dataKey="name" {...axis} interval={0} tick={{ fontSize: 11 }} height={48} />
                  <YAxis {...axis} width={44} domain={[0, 1]} />
                  <Tooltip {...tip} formatter={(v: number) => [v.toFixed(3), "F1"]} />
                  <Bar dataKey="f1" isAnimationActive={false} radius={[4, 4, 0, 0]} maxBarSize={64} label={{ position: "top", fill: "var(--ink)", fontSize: 11, formatter: (v: number) => v.toFixed(2) }}>
                    {clfOrder.map((k) => (
                      <Cell key={k} fill={COLOR[k]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </Card>
          </div>
        )}
      </Block>

      <Block title="What the model uses" lead="Importance of each feature in the full gradient-boosted trees. It shows what the trees split on, not what causes delay.">
        <div className="card">
          <ResponsiveContainer width="100%" height={Math.max(240, imp.length * 28 + 50)}>
            <BarChart data={imp} layout="vertical" margin={{ left: 0, right: 50, bottom: 14 }}>
              <CartesianGrid stroke="var(--grid)" horizontal={false} />
              <XAxis type="number" {...axis} label={xLabel("Share of total importance")} height={48} />
              <YAxis type="category" dataKey="feature" {...axis} width={220} tick={{ fontSize: 12 }} />
              <Tooltip {...tip} formatter={(v: number) => [v.toFixed(3), "Importance"]} />
              <Bar dataKey="importance" fill={task === "regression" ? C.blue : C.vermillion} isAnimationActive={false} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </Block>

      <div className="callout warm">
        <b>Read this honestly.</b> These are next-timepoint predictions made when the bus leaves the previous timepoint, not day-ahead forecasts. Most of the skill comes from the previous delay: without it the model's mean error is {fmtNum(reg[NOPREV].mae_min, 2)} min against {fmtNum(reg[MEAN].mae_min, 2)} min for guessing the average. The models were tested on later days of the same four months of 2024 in Boston and say nothing about other months or cities.
      </div>
    </>
  );
}

function clfVals(m: ClfMetrics): string[] {
  return [fmtPct(m.accuracy), fmtPct(m.precision), fmtPct(m.recall), fmtNum(m.f1, 3), m.auc == null ? "n/a" : fmtNum(m.auc, 3)];
}

function MetricTable({ rows, heads }: { rows: { name: string; key: string; vals: string[] }[]; heads: string[] }) {
  return (
    <table className="mini data">
      <thead>
        <tr>
          <th>Model</th>
          {heads.map((h) => (
            <th key={h} className="num">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.key} className={r.key === FULL ? "hl" : ""}>
            <td>{r.name}</td>
            {r.vals.map((v, i) => (
              <td key={i} className="num">
                {v}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export type { RegMetrics };
