import { useMemo, useState } from "react";
import { sortRows, type SortDir } from "../lib/format";

export interface Column<T> {
  key: keyof T & string;
  label: string;
  num?: boolean;
  render?: (row: T) => React.ReactNode;
}

export function DataTable<T extends object>({ rows, columns, defaultSort, defaultDir = "desc", pageSize = 10, empty = "No rows match the current filters." }: {
  rows: T[];
  columns: Column<T>[];
  defaultSort: keyof T & string;
  defaultDir?: SortDir;
  pageSize?: number;
  empty?: string;
}) {
  const [sortKey, setSortKey] = useState<keyof T & string>(defaultSort);
  const [dir, setDir] = useState<SortDir>(defaultDir);
  const [page, setPage] = useState(0);

  const sorted = useMemo(() => sortRows(rows, sortKey, dir), [rows, sortKey, dir]);
  const pages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const cur = Math.min(page, pages - 1);
  const slice = sorted.slice(cur * pageSize, cur * pageSize + pageSize);

  const clickHeader = (k: keyof T & string) => {
    if (k === sortKey) setDir(dir === "asc" ? "desc" : "asc");
    else {
      setSortKey(k);
      setDir("desc");
    }
    setPage(0);
  };

  return (
    <div>
      <div className="tablewrap">
        <table>
          <thead>
            <tr>
              {columns.map((c) => (
                <th key={c.key} className={c.num ? "num" : ""} onClick={() => clickHeader(c.key)} aria-sort={c.key === sortKey ? (dir === "asc" ? "ascending" : "descending") : "none"}>
                  {c.label}
                  {c.key === sortKey ? (dir === "asc" ? " ▲" : " ▼") : ""}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {slice.map((r, i) => (
              <tr key={i}>
                {columns.map((c) => (
                  <td key={c.key} className={c.num ? "num" : ""}>
                    {c.render ? c.render(r) : String((r as Record<string, unknown>)[c.key] ?? "n/a")}
                  </td>
                ))}
              </tr>
            ))}
            {slice.length === 0 && (
              <tr>
                <td colSpan={columns.length} style={{ textAlign: "center", color: "var(--muted)" }}>
                  {empty}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="pager">
        <span>
          {sorted.length.toLocaleString("en-US")} rows · page {cur + 1} of {pages}
        </span>
        <span style={{ display: "flex", gap: 6 }}>
          <button disabled={cur === 0} onClick={() => setPage(cur - 1)}>
            Prev
          </button>
          <button disabled={cur >= pages - 1} onClick={() => setPage(cur + 1)}>
            Next
          </button>
        </span>
      </div>
    </div>
  );
}
