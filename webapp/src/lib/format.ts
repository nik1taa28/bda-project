// Small, pure formatting and data-shaping helpers (unit-tested in format.test.ts)

export const fmtInt = (n: number | null | undefined): string =>
  n == null ? "n/a" : Math.round(n).toLocaleString("en-US");

export const fmtNum = (n: number | null | undefined, digits = 2): string =>
  n == null || Number.isNaN(n) ? "n/a" : n.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });

/** fraction (0..1) -> "55.4%" */
export const fmtPct = (frac: number | null | undefined, digits = 1): string =>
  frac == null || Number.isNaN(frac) ? "n/a" : `${(frac * 100).toFixed(digits)}%`;

/** seconds -> minutes with one decimal, e.g. 259 -> "4.3 min" */
export const fmtMin = (sec: number | null | undefined, digits = 1): string =>
  sec == null || Number.isNaN(sec) ? "n/a" : `${(sec / 60).toFixed(digits)} min`;

export const fmtBytes = (n: number | null | undefined): string => {
  if (n == null) return "not measured";
  const units = ["B", "KB", "MB", "GB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
};

export interface Hist {
  lo: number;
  hi: number;
  width: number;
  counts: number[];
  n: number;
}

/** Histogram -> chart rows. x = bin centre; y = share of all values in the bin (so two groups of different size compare fairly). */
export function histSeries(h: Hist): { x: number; y: number; count: number }[] {
  return h.counts.map((c, i) => ({ x: +(h.lo + (i + 0.5) * h.width).toFixed(4), y: h.n ? c / h.n : 0, count: c }));
}

export function median(xs: number[]): number | null {
  const v = xs.filter((x) => x != null && !Number.isNaN(x)).sort((a, b) => a - b);
  if (!v.length) return null;
  const m = Math.floor(v.length / 2);
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}

export type SortDir = "asc" | "desc";

/** Sort rows by a numeric/text key; null and undefined always go last. */
export function sortRows<T>(rows: T[], key: keyof T, dir: SortDir): T[] {
  const sign = dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const x = a[key] as unknown;
    const y = b[key] as unknown;
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    if (typeof x === "number" && typeof y === "number") return (x - y) * sign;
    return String(x).localeCompare(String(y)) * sign;
  });
}

export function matches(text: string, query: string): boolean {
  const q = query.trim().toLowerCase();
  return q === "" || text.toLowerCase().includes(q);
}
