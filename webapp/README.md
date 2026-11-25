# Bus Analytics Dashboard

An interactive, static web dashboard for the PySpark project in the repository root. It needs **no server, no Spark and no Python** to run: it reads small JSON files exported from the project's measured results.

| Tab | What you can explore |
|---|---|
| **Delay explorer** | Real departure delay (actual minus scheduled): distribution, on-time share by hour, route explorer, delay by position in the trip, scheduled vs actual slowdown, headway bunching |
| **Delay prediction (ML)** | Spark MLlib results: gradient-boosted trees vs simple baselines on held-out days (delay in minutes and late / not late), calibration, confusion matrix, error by hour, feature importance, how the split and features were built |
| **Pipeline & performance** | Clickable pipeline stages from raw CSV to predictions, data-completeness waterfall, 1/2/4-core speedup and efficiency for the analysis and for model training, broadcast vs shuffle join, correctness checks, test machine |

> All pages are about **Boston MBTA bus data (2024)**: real scheduled and actual times. The prediction tab is a next-timepoint forecast evaluated on later days of the same months.

## Run it

```bash
cd webapp
npm ci
npm run dev        # http://localhost:5173
```

Production build and local preview:

```bash
npm run build      # type-checks, then writes webapp/dist
npm run preview    # http://localhost:4173
```

The built site uses relative paths, so it works from any folder or from a GitHub Pages project site.

## Where the data comes from

`public/data/*.json` are generated from `outputs/` (CSV, Parquet and JSON written by the pipelines, benchmarks and tests):

```bash
python scripts/export_webapp_data.py     # from the repository root
```

Every number shown comes from those files; nothing is typed in by hand. `tests/test_webapp_export.py` checks that the JSON matches the sources and is not stale. Re-run the export after re-running any pipeline or benchmark.

## Stack

React 18, TypeScript, Vite, Recharts. Plain CSS with light and dark themes, colour-blind-safe Okabe-Ito colours (the same as the PNG figures in `outputs/figures/`). Unit tests: `npm test` (Vitest).

## Publish on GitHub Pages

`.github/workflows/deploy-pages.yml` builds and deploys the site on every push to `main` that touches `webapp/`. Enable it once under **Settings, Pages, Source: GitHub Actions**.
