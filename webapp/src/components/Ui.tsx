import type { ReactNode } from "react";

export function ScopeBanner({ kind, title, children }: { kind: "boston"; title: string; children: ReactNode }) {
  return (
    <div className={`scope ${kind}`} role="note">
      <span className="pill">{title}</span>
      <div>{children}</div>
    </div>
  );
}

export function Kpi({ label, value, hint, accent }: { label: string; value: string; hint?: string; accent?: string }) {
  return (
    <div className="kpi" style={accent ? ({ "--accent": accent } as React.CSSProperties) : undefined}>
      <div className="v">{value}</div>
      <div className="l">{label}</div>
      {hint && <div className="h">{hint}</div>}
    </div>
  );
}

export function Card({ title, sub, note, children }: { title: string; sub?: string; note?: ReactNode; children: ReactNode }) {
  return (
    <div className="card">
      <h3>{title}</h3>
      {sub && <p className="sub">{sub}</p>}
      {children}
      {note && <p className="note">{note}</p>}
    </div>
  );
}

export function Segmented<T extends string>({ options, value, onChange, label }: { options: { value: T; label: string }[]; value: T; onChange: (v: T) => void; label: string }) {
  return (
    <div className="seg" role="group" aria-label={label}>
      {options.map((o) => (
        <button key={o.value} aria-pressed={o.value === value} onClick={() => onChange(o.value)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Slider({ label, min, max, step = 1, value, onChange }: { label: string; min: number; max: number; step?: number; value: number; onChange: (v: number) => void }) {
  return (
    <label className="field">
      {label}
      <input type="range" min={min} max={max} step={step} value={value} onChange={(e) => onChange(Number(e.target.value))} />
      <output>{value}</output>
    </label>
  );
}

export function Block({ title, lead, children }: { title: string; lead?: string; children: ReactNode }) {
  return (
    <section className="block">
      <h2>{title}</h2>
      {lead && <p className="lead">{lead}</p>}
      {children}
    </section>
  );
}
