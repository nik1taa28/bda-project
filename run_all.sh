#!/usr/bin/env bash
# End-to-end: download -> delay pipeline -> ML models -> tests -> benchmark -> figures -> docs -> dashboard data.
# MODEL_BENCH=0 ./run_all.sh   skips the (long) model-training benchmark.
# WEBAPP=0 ./run_all.sh        skips exporting the dashboard data (webapp/public/data).
set -euo pipefail
cd "$(dirname "$0")"
[ -d .venv ] || { python3.11 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
PY=.venv/bin/python
java -version 2>&1 | head -1
$PY -m src.environment > /dev/null
$PY scripts/download_delay_data.py
$PY -m src.delay_pipeline
$PY -m src.delay_model
$PY -m pytest -q
if [ "${MODEL_BENCH:-1}" = "1" ]; then $PY -m src.benchmark; else $PY -m src.benchmark --skip-model; fi
$PY -m src.viz
$PY -m src.make_docs
[ "${WEBAPP:-1}" = "1" ] && $PY scripts/export_webapp_data.py
echo "done: see README.md and outputs/RESULTS.md"
