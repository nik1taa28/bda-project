# Proposal wording updates

The project changed after the first proposal: the Bengaluru (BMTC) static-timetable analysis was dropped because a static GTFS feed has no actual arrival times, so delay cannot be measured, and a
**delay prediction model** was added. This file gives the replacement text so the Word proposal matches what the code does. Numbers come from `outputs/`.
(The BMTC work remains available in the git history of this repository.)

| # | Replace | With |
|---|---|---|
| 1 | Title (BMTC, scheduled travel-time variability) | "Scalable Bus Delay Analytics and Delay Prediction on Boston MBTA Data using PySpark" |
| 2 | "BMTC GTFS", "Bengaluru" anywhere in the text | "MBTA (Boston) Bus Arrival Departure Times 2024", the public open dataset with both scheduled and actual times; licence on the item: CC0 |
| 3 | Research questions on scheduled variability and feeder vs trunk | Q1: "How late are buses, when and where (on-time share, delay by hour, route and position in the trip), and how much bunching is there?" Q2: "Can Spark MLlib models predict the delay (minutes) and whether a bus will be late (> 5 min) better than simple baselines on held-out days?" Q3: "What speedup and throughput does PySpark achieve with 1, 2 and 4 cores for the analysis pipeline and for model training, and how do broadcast and shuffle joins compare?" |
| 4 | "~250 MB / ~1.5M records" | "9,108,352 raw arrival/departure events from 4 months of 2024 (Jan, Apr, Jul, Oct), about 1.0 GB of CSV; 8,424,946 cleaned events after quality filters" |
| 5 | `.txt` GTFS file names, `unix_timestamp` code outline | "Monthly CSV files read with an explicit all-text schema; `scheduled` and `actual` are time-of-day stamps parsed with a regex-based expression; delay = actual - scheduled modulo 24 h (`F.pmod`) so after-midnight trips are correct" |
| 6 | Window / join outline | "`Window.partitionBy(service_date, half_trip_id).orderBy(time_point_order)` with `lag` gives the previous timepoint (run time between timepoints and the previous delay used as a model feature). Route and route+timepoint history tables computed on training days are broadcast-joined (broadcast vs sort-merge compared)." |
| 7 | (new) Machine learning | "Spark MLlib gradient-boosted trees (`GBTRegressor`, `GBTClassifier`) with `StringIndexer` and `VectorAssembler`. Temporal split (the last 25% of the days of each month held out), baselines (train mean, previous delay carried forward, never late), and an ablation without the previous-delay feature." |
| 8 | "8-core / 16 GB" | "Apple M3, 8 cores (4 performance + 4 efficiency), 16.0 GB RAM, Darwin 23.3.0 (arm64), Java openjdk version "17.0.21" 2026-10-20, Python 3.11.13, PySpark 3.5.3" |
| 9 | Evaluation (speedup, latency) | "Speedup, latency and efficiency on 1, 2 and 4 cores for the pipeline and for model training; broadcast vs shuffle join with plans and shuffle bytes; completeness waterfall; pandas cross-check of the Spark delay aggregates" |
| 10 | Any claim about Bengaluru / BMTC results | remove; if BMTC is mentioned, say only that BMTC publishes a static timetable and no actual times, which is why a public dataset with actual times was used |
| 11 | (new limitation) | "Boston only; timepoints only; four sampled months of 2024; Schedule-standard trips only; next-timepoint prediction (uses the previous timepoint's delay); single-machine `local[N]` benchmarks" |
