import { useEffect, useState } from "react";
import type { Hist } from "./format";

// ---- shapes of the JSON files written by scripts/export_webapp_data.py ------------------------------------
export interface WaterfallRow {
  stage: string;
  rows_in: number;
  rows_out: number;
  pct_retained: number;
  reason: string;
  kind: "start" | "quality" | "scope" | "structural" | "final";
}

export interface DelayStats {
  n: number;
  mean_delay_sec: number;
  median_delay_sec: number;
  p90_delay_sec: number;
  std_delay_sec: number;
  on_time_rate: number;
  late_rate: number;
  early_rate: number;
}

export interface Boston {
  overall: DelayStats;
  hop_overall: { n: number; mean_sched_hop: number; mean_act_hop: number; mean_added_sec: number };
  by_hour: (DelayStats & { hour_of_day: number })[];
  by_route: (DelayStats & { route_id: string })[];
  by_daytype_period: (DelayStats & { day_type: string; period: string })[];
  by_point_type: (DelayStats & { point_type: string })[];
  by_timepoint_order: (DelayStats & { time_point_order: number })[];
  hop_by_hour: { hour_of_day: number; n: number; mean_sched_hop: number; mean_act_hop: number; mean_added_sec: number }[];
  headway_overall: { n: number; mean_headway_ratio: number; bunched_rate: number; gap_rate: number };
  headway_by_hour: { hour_of_day: number; n: number; mean_headway_ratio: number; bunched_rate: number; gap_rate: number }[];
  headway_by_route: { route_id: string; n: number; mean_headway_ratio: number; bunched_rate: number; gap_rate: number }[];
  delay_hist_min: Hist;
  seg_slowdown: { route_id: string; direction_id: string; from_tp: string; time_point_id: string; PEAK_n: number; OFF_PEAK_n: number; sched_slowdown_ratio: number; actual_slowdown_ratio: number }[];
  waterfall: WaterfallRow[];
  settings: { months: number[]; on_time_early_sec: number; on_time_late_sec: number; max_abs_delay_sec: number; min_events: number; headway_bunch_ratio: number; headway_gap_ratio: number };
}

export interface ScalingEntry {
  oversubscribed: boolean;
  total_latency_s_median: number;
  throughput_rows_per_s?: number;
  speedup_total: number;
  efficiency_total: number;
  stages_seconds: Record<string, { median: number; min: number; max: number }>;
  speedup_by_stage: Record<string, number>;
}

export interface JoinVariant {
  join_seconds: { median: number; min: number; max: number };
  shuffle_write_bytes_per_run_median: number | string;
}

export interface Pipeline {
  benchmark: {
    environment: Environment;
    config: { timed_runs: number; warmup_runs: number; shuffle_partitions: number; months: number[] };
    cleaned_event_rows: number;
    scaling: Record<string, ScalingEntry>;
    join_comparison: { cores: number; what: string; variants: Record<"broadcast" | "shuffle", JoinVariant> };
    model_scaling?: {
      timed_runs: number;
      warmup_runs: number;
      train_rows_used: number;
      test_rows: number;
      gbt: { gbt_max_iter: number; gbt_max_depth: number; max_train_rows: number };
      scaling: Record<string, ScalingEntry>;
    };
  };
  environment: Environment;
  correctness_delay: { routes_checked: string[]; events_compared: number; max_abs_diff_mean_std_on_time: number; passed: boolean } | null;
  waterfall: WaterfallRow[];
  time_buckets: Record<string, number[]>;
  delay_model: { test_fraction: number; max_train_rows: number; gbt_max_iter: number; gbt_max_depth: number; max_bins: number; seed: number };
}

export interface RegMetrics {
  rmse_min: number;
  mae_min: number;
  r2: number;
}

export interface ClfMetrics {
  accuracy: number;
  precision: number;
  recall: number;
  f1: number;
  auc: number | null;
  tp: number;
  fp: number;
  fn: number;
  tn: number;
}

export interface SplitInfo {
  rows: number;
  first_date: string;
  last_date: string;
  days: number;
  late_rate: number;
  mean_delay_min: number;
}

export interface Model {
  results: {
    split: { train: SplitInfo; test: SplitInfo };
    train_rows_used: number;
    train_cap: number;
    test_rows: number;
    n_routes: number;
    test_fraction: number;
    late_threshold_sec: number;
    gbt: { gbt_max_iter: number; gbt_max_depth: number; seed: number };
    regression: Record<string, RegMetrics>;
    classification: Record<string, ClfMetrics>;
    fits: Record<string, { fit_seconds: number }>;
    features_full: string[];
    features_without_previous_delay: string[];
    prepare_seconds: number;
    total_seconds: number;
  };
  regression_calibration: { bin: number; n: number; mean_predicted_min: number; mean_actual_min: number }[];
  classification_calibration: { bin: number; n: number; mean_predicted_prob: number; actual_late_rate: number }[];
  error_by_hour: { hour_of_day: number; n: number; gbt_mae_min: number; mean_baseline_mae_min: number; carry_baseline_mae_min: number }[];
  feature_importance: { model: string; task: "regression" | "classification"; feature: string; importance: number }[];
  /** [actual delay min, predicted delay min, late (0/1), predicted P(late)] */
  prediction_sample: [number, number, number, number][];
}

export interface Environment {
  os: string;
  cpu: string;
  logical_cores: number;
  performance_cores: number | null;
  efficiency_cores: number | null;
  ram_gb: number;
  python: string;
  java: string;
  pyspark: string;
}

export interface Meta {
  exported_on: string;
  boston: { title: string | null; licence: string | null; months: number[] };
}

export interface AllData {
  meta: Meta;
  boston: Boston;
  model: Model;
  pipeline: Pipeline;
}

// ---- loading ---------------------------------------------------------------------------------------------
const base = import.meta.env.BASE_URL;

async function getJson<T>(name: string): Promise<T> {
  const res = await fetch(`${base}data/${name}.json`);
  if (!res.ok) throw new Error(`Could not load ${name}.json (${res.status}). Run scripts/export_webapp_data.py first.`);
  return (await res.json()) as T;
}

export type DataState = { status: "loading" } | { status: "error"; message: string } | { status: "ready"; data: AllData };

export function useData(): DataState {
  const [state, setState] = useState<DataState>({ status: "loading" });
  useEffect(() => {
    let alive = true;
    Promise.all([getJson<Meta>("meta"), getJson<Boston>("boston"), getJson<Model>("model"), getJson<Pipeline>("pipeline")])
      .then(([meta, boston, model, pipeline]) => alive && setState({ status: "ready", data: { meta, boston, model, pipeline } }))
      .catch((e: Error) => alive && setState({ status: "error", message: e.message }));
    return () => {
      alive = false;
    };
  }, []);
  return state;
}
