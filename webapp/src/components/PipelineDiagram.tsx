import { useState } from "react";
import type { WaterfallRow } from "../lib/data";
import { fmtInt } from "../lib/format";

interface Step {
  id: string;
  title: string;
  file: string;
  funcs: string;
  what: string[];
  /** indices into the waterfall array that this step spans (inclusive), used to show measured rows in/out */
  span?: [number, number];
}

const STEPS: Step[] = [
  { id: "1", title: "Ingest", file: "src/delay_pipeline.py", funcs: "stage_delay_clean, input_files", span: [0, 0],
    what: ["Reads the 4 monthly CSVs; all columns as text, cast explicitly (no inferSchema)", "Trim spaces, blank strings become null"] },
  { id: "2", title: "Clean", file: "src/delay_pipeline.py + src/waterfall.py", funcs: "stage_delay_clean, iso_to_sec, Waterfall", span: [1, 3],
    what: ["Drop null keys and duplicate events", "Parse 1900-01-01T06:05:03Z into seconds of day", "Every step is counted in the row-count waterfall"] },
  { id: "3", title: "Delay", file: "src/delay_pipeline.py", funcs: "wrap_delay", span: [4, 5],
    what: ["Drop events with no actual time", "delay = actual minus scheduled, modulo 24 h (midnight-safe)", "Drop |delay| above 2 hours as errors"] },
  { id: "4", title: "Aggregate", file: "src/delay_pipeline.py", funcs: "stage_delay_agg", span: [6, 6],
    what: ["Keep Schedule-standard trips for delay", "On-time, late, early share by hour, route, day type", "lag() over timepoints: actual vs scheduled run time; headway bunching"] },
  { id: "5", title: "Features", file: "src/delay_model.py", funcs: "add_trip_features, test_flag, add_history",
    what: ["lag() gives the delay at the previous timepoint of the same trip (earlier events only)", "Temporal split: the last 25% of the days of each month are the test set", "Route and route+timepoint averages from training days only, broadcast-joined"] },
  { id: "6", title: "Train + evaluate", file: "src/delay_model.py", funcs: "prepare, fit_gbt, run",
    what: ["StringIndexer + VectorAssembler + GBTRegressor / GBTClassifier (Spark MLlib)", "Compared with baselines: train mean, previous delay carried forward, never late", "Metrics on the held-out days only; an ablation drops the previous-delay feature"] },
  { id: "7", title: "Outputs", file: "src/viz.py, src/make_docs.py", funcs: "fig_*, save_table", what: ["CSV and Parquet tables, PNG figures", "Generated README and RESULTS.md", "JSON export for this dashboard"] },
];

export function PipelineDiagram({ waterfall }: { waterfall: WaterfallRow[] }) {
  const steps = STEPS;
  const [sel, setSel] = useState(0);
  const s = steps[Math.min(sel, steps.length - 1)];
  const span = s.span ? waterfall.slice(s.span[0], s.span[1] + 1) : [];
  const rowsIn = span.length ? span[0].rows_in : null;
  const rowsOut = span.length ? span[span.length - 1].rows_out : null;
  return (
    <div>
      <div className="flow" role="group" aria-label="Pipeline stages">
        {steps.map((st, i) => (
          <button key={st.id} className="step" aria-pressed={i === sel} onClick={() => setSel(i)}>
            <div className="n">STEP {st.id}</div>
            <div className="t">{st.title}</div>
          </button>
        ))}
      </div>
      <div className="detail">
        <div>
          <h4>
            {s.title} <span className="tag">{s.file}</span>
          </h4>
          <p className="mini" style={{ marginTop: 6 }}>
            Functions: <code>{s.funcs}</code>
          </p>
          <ul>
            {s.what.map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        </div>
        <div>
          <h4>Measured rows</h4>
          {span.length ? (
            <>
              <dl className="kv">
                <dt>Rows in</dt>
                <dd>{fmtInt(rowsIn)}</dd>
                <dt>Rows out</dt>
                <dd>{fmtInt(rowsOut)}</dd>
              </dl>
              <table className="mini" style={{ marginTop: 8 }}>
                <tbody>
                  {span.map((r) => (
                    <tr key={r.stage}>
                      <td>{r.stage.replace(/^\d+\.\s*/, "")}</td>
                      <td className="num">{r.pct_retained.toFixed(2)}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          ) : (
            <p className="mini">This step writes files rather than removing rows.</p>
          )}
        </div>
      </div>
    </div>
  );
}
