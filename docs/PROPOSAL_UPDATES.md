# Proposal wording updates

Exact replacement text so the Word proposal matches what the code does. Numbers come from `DATA_CARD.md` / `outputs/`.

| # | Replace | With |
|---|---|---|
| 1 | "schedule delay", "actual vs scheduled", ">20 minutes deviation" | "scheduled travel-time variability (standard deviation and coefficient of variation of planned hop and trip times across trips), scheduled hop speed, peak vs off-peak scheduled slowdown, and scheduled headway variability. Static GTFS holds planned times only, so actual delay cannot be measured." |
| 2 | Research questions on delay hotspots | Q1: "Which route segments and corridors have the highest scheduled peak-hour travel-time variability and the largest peak vs off-peak scheduled slowdown?" Q2: "How do scheduled variability and hop speed compare between metro-feeder routes and long trunk routes?" Q3: "What speedup and throughput does PySpark achieve with 1, 2 and 4 cores, and how do broadcast and shuffle joins compare?" |
| 3 | "~250 MB / ~1.5M records" | "a snapshot taken on 2026-11-16: 2,238,693 stop_times rows (132 MB), 69,642 trips, 5,063 routes, 10,128 stops; about 140 MB for the five files used (477 MB of GTFS text files in total). The feed changes over time." |
| 4 | "ODbL" | "ODC Open Database License (ODbL) (as stated in the source repository's LICENSE file)" |
| 5 | `.csv` file names | GTFS `.txt` files: `routes.txt`, `trips.txt`, `stop_times.txt`, `stops.txt`, `calendar.txt` (`calendar_dates.txt` is not in this snapshot) |
| 6 | `unix_timestamp(col, "HH:mm:ss")` in the code outline | "A custom GTFS time parser (regex check, then hours*3600 + minutes*60 + seconds) because GTFS times can exceed 24:00:00. Hop time = arrival seconds at stop i minus departure seconds at stop i-1 within the same trip (`Window.partitionBy(trip_id).orderBy(stop_sequence)` with `lag`); dwell = departure minus arrival." |
| 7 | "8-core / 16 GB" | "Apple M3, 8 cores (4 performance + 4 efficiency), 16.0 GB RAM, Darwin 23.3.0 (arm64), Java openjdk version "17.0.21" 2026-10-20, Python 3.11.13, PySpark 3.5.3" |
| 8 | (new limitation) | "The timetable is scraped from an unofficial app source whose maintainer calls it unreliable, and the times appear interpolated at near-constant speed (93.8% of hops are within 20% of their trip's median speed), so scheduled variability is expected to be low." |
| 9 | Segment definition by `route_long_name, stop_id` | "A segment is (route_id, direction_id, from_stop_id, to_stop_id); a stop-pair rollup across routes is also reported." |
| 10 | "corridor" / named junctions | "Named hotspots are found by matching stop names (e.g. Silk Board, Tin Factory, Hebbal); only names present in the data are reported." |
| 12 | "actual delay at BMTC stops" (any claim of measured delay) | "BMTC publishes no actual arrival times, so BMTC delay cannot be measured. Actual-vs-scheduled delay is demonstrated on the public MBTA (Boston) bus arrival/departure dataset (licence on the item: CC0), using the same PySpark pipeline design; those results describe Boston, not Bengaluru." |
| 13 | Objectives / Q3 evaluation data | add: "The pipeline is also benchmarked on 1, 2 and 4 cores on 8,424,946 real delay events (4 months of 2024), in addition to the Bengaluru timetable." |
| 11 | "metro-feeder / trunk" | "Heuristic classes: feeder = route name prefix `MF-`; trunk = non-feeder with median trip length at least 30 km." |
