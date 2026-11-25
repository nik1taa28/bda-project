import { describe, expect, it } from "vitest";
import { fmtBytes, fmtInt, fmtMin, fmtPct, histSeries, matches, median, sortRows } from "./format";

describe("formatters", () => {
  it("formats integers with separators", () => expect(fmtInt(2238693)).toBe("2,238,693"));
  it("formats a fraction as a percent", () => expect(fmtPct(0.55412)).toBe("55.4%"));
  it("formats seconds as minutes", () => expect(fmtMin(258.8)).toBe("4.3 min"));
  it("handles missing values", () => {
    expect(fmtInt(null)).toBe("n/a");
    expect(fmtPct(undefined)).toBe("n/a");
    expect(fmtBytes(null)).toBe("not measured");
  });
  it("formats bytes", () => expect(fmtBytes(219268237)).toBe("209.1 MB"));
});

describe("data helpers", () => {
  it("builds histogram rows as shares", () => {
    const rows = histSeries({ lo: 0, hi: 20, width: 10, counts: [1, 3], n: 4 });
    expect(rows).toEqual([
      { x: 5, y: 0.25, count: 1 },
      { x: 15, y: 0.75, count: 3 },
    ]);
  });
  it("computes the median", () => {
    expect(median([3, 1, 2])).toBe(2);
    expect(median([4, 1, 2, 3])).toBe(2.5);
    expect(median([])).toBeNull();
  });
  it("sorts with nulls last in both directions", () => {
    const rows = [{ v: 2 }, { v: null as number | null }, { v: 5 }];
    expect(sortRows(rows, "v", "desc").map((r) => r.v)).toEqual([5, 2, null]);
    expect(sortRows(rows, "v", "asc").map((r) => r.v)).toEqual([2, 5, null]);
  });
  it("matches case-insensitively", () => {
    expect(matches("Silk Board", "silk")).toBe(true);
    expect(matches("Hebbal", "silk")).toBe(false);
    expect(matches("anything", "  ")).toBe(true);
  });
});
