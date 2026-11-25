import { useEffect, useState } from "react";
import { useData } from "./lib/data";
import { fmtInt } from "./lib/format";
import { BostonPage } from "./pages/Boston";
import { PredictionPage } from "./pages/Prediction";
import { PipelinePage } from "./pages/Pipeline";

type Tab = "delay" | "prediction" | "pipeline";
const TABS: { id: Tab; label: string }[] = [
  { id: "delay", label: "Delay explorer" },
  { id: "prediction", label: "Delay prediction (ML)" },
  { id: "pipeline", label: "Pipeline & performance" },
];

const fromHash = (): Tab => {
  const h = window.location.hash.replace("#", "");
  return (TABS.find((t) => t.id === h)?.id ?? "delay") as Tab;
};

function safeGet(k: string): string | null {
  try {
    return window.localStorage.getItem(k);
  } catch {
    return null;
  }
}

export default function App() {
  const state = useData();
  const [tab, setTab] = useState<Tab>(fromHash);
  const [theme, setTheme] = useState<string | null>(() => safeGet("theme"));

  useEffect(() => {
    const on = () => setTab(fromHash());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);

  useEffect(() => {
    const el = document.documentElement;
    if (theme) el.setAttribute("data-theme", theme);
    else el.removeAttribute("data-theme");
    try {
      if (theme) window.localStorage.setItem("theme", theme);
    } catch {
      /* storage unavailable: theme simply is not remembered */
    }
  }, [theme]);

  const go = (t: Tab) => {
    window.location.hash = t;
    setTab(t);
    window.scrollTo({ top: 0 });
  };
  const dark = theme ? theme === "dark" : window.matchMedia?.("(prefers-color-scheme: dark)").matches;

  return (
    <>
      <header className="top">
        <div className="wrap top-in">
          <div className="brand">
            <span className="brand-mark" aria-hidden>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M4 16V8a3 3 0 0 1 3-3h10a3 3 0 0 1 3 3v8M4 16h16M4 16v2M20 16v2M8 12h8" />
              </svg>
            </span>
            <span>
              Bus Analytics
              <small>PySpark · Boston MBTA</small>
            </span>
          </div>
          <nav className="tabs" role="tablist" aria-label="Sections">
            {TABS.map((t) => (
              <button key={t.id} role="tab" className="tab" aria-selected={tab === t.id} onClick={() => go(t.id)}>
                {t.label}
              </button>
            ))}
          </nav>
          <button className="theme" onClick={() => setTheme(dark ? "light" : "dark")} aria-label="Toggle dark mode" title="Toggle dark mode">
            {dark ? "Light" : "Dark"}
          </button>
        </div>
      </header>

      <main className="wrap">
        <div className="hero">
          <h1>Bus delay analytics and prediction with PySpark</h1>
          <p>
            Real scheduled-vs-actual bus times from Boston (MBTA, 2024): how late buses are, when and where, and whether Spark MLlib models can predict the delay.
            {state.status === "ready" && (
              <>
                {" "}
                Data: {fmtInt(state.data.pipeline.waterfall[0].rows_out)} raw events, {fmtInt(state.data.model.results.test_rows)} held-out test events.
              </>
            )}
          </p>
        </div>

        {state.status === "loading" && <div className="state">Loading results...</div>}
        {state.status === "error" && (
          <div className="state" role="alert">
            {state.message}
          </div>
        )}
        {state.status === "ready" && (
          <>
            {tab === "delay" && <BostonPage d={state.data.boston} />}
            {tab === "prediction" && <PredictionPage d={state.data.model} />}
            {tab === "pipeline" && <PipelinePage d={state.data.pipeline} />}
            <footer>
              Results exported {state.data.meta.exported_on} from the project's measured outputs. Boston data: MBTA Open Data Portal ({state.data.meta.boston.licence}). Course project, CSE557 Big Data Analytics.
            </footer>
          </>
        )}
      </main>
    </>
  );
}
