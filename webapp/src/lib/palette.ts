// Okabe-Ito colour-blind-safe palette (same colours as the PNG figures made by src/viz.py)
export const C = {
  blue: "#0072B2",
  orange: "#E69F00",
  green: "#009E73",
  vermillion: "#D55E00",
  sky: "#56B4E9",
  pink: "#CC79A7",
  yellow: "#F0E442",
  grey: "#7F7F7F",
} as const;

// A sequential ramp (light -> dark blue) for the stop map; validated for colour-blind readability.
export const RAMP = ["#e3f0fa", "#b6d6ee", "#7db6dd", "#3f8fc4", "#0072B2", "#00507d", "#002f4d"];

export function rampColor(t: number): string {
  const x = Math.min(1, Math.max(0, t)) * (RAMP.length - 1);
  const i = Math.floor(x);
  const j = Math.min(RAMP.length - 1, i + 1);
  const f = x - i;
  const a = hex(RAMP[i]);
  const b = hex(RAMP[j]);
  const m = a.map((v, k) => Math.round(v + (b[k] - v) * f));
  return `rgb(${m[0]},${m[1]},${m[2]})`;
}

function hex(h: string): number[] {
  return [1, 3, 5].map((k) => parseInt(h.slice(k, k + 2), 16));
}
