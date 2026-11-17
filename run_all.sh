#!/usr/bin/env bash
# End-to-end: download -> pipeline -> profile -> tests -> benchmark -> figures -> docs.
# SYNTHETIC=0 ./run_all.sh   skips the long synthetic scale-up test.
# DELAY=0 ./run_all.sh       skips the real-delay (Boston MBTA) analysis and its benchmark.
set -euo pipefail
cd "$(dirname "$0")"
[ -d .venv ] || { python3.11 -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
PY=.venv/bin/python
java -version 2>&1 | head -1
$PY -m src.environment > /dev/null
$PY scripts/download_data.py
$PY -m src.pipeline --service-day weekday
$PY -m src.profile_data > /dev/null
if [ "${DELAY:-1}" = "1" ]; then
  $PY scripts/download_delay_data.py
  $PY -m src.delay_pipeline
fi
$PY -m pytest -q
DW=""; [ "${DELAY:-1}" = "1" ] && DW="--with-delay"
if [ "${SYNTHETIC:-1}" = "1" ]; then $PY -m src.benchmark --synthetic $DW; else $PY -m src.benchmark $DW; fi
$PY -m src.viz
$PY -m src.make_docs
echo "done: see README.md and outputs/RESULTS.md"
