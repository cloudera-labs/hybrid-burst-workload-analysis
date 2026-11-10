# Copyright 2026 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Re-derive every TPC-DS table the aggregation study reports, from a durable script.

The production tables come from ``verify_aggregation`` and ``group_stability``. The
TPC-DS tables were first produced by throwaway scripts, which is not good enough to
cite: a number in a manuscript has to be reproducible by running something that is
committed. This module is that something. It reproduces, in order:

  1. The arms on TPC-DS, so the production result has a benchmark comparison.
  2. The two saturated-case forms, the printed stage-level sum against the wave
     packing the released implementation computes, and how often they diverge.
  3. Signed error for both forms, so the bias direction is on record.
  4. Stage-graph sparsity, TPC-DS against the production trace.
  5. The arms re-scored inside each interconnect configuration (the invariance table).
  6. The transfer-stage share, and the arms restricted to transfer-dominated runs
     (the dilution control that the invariance claim requires).

The TPC-DS data is the dataset released with the transport-aware model. It lives in
a different repository and is *read only* here: this script never writes into it, so
the published artifact stays exactly as published.

    --tpcds-dir   the model repo's data directory, holding
                  enriched_measurement_summary.json,
                  enriched_stage_measurement_summary.json, and graph/<fmt>_<codec>/

Every arm is fed the observed ``stage_duration_s`` and predicts the observed
``exec_time_s``, so the aggregation is the only thing that varies. That is the same
protocol ``verify_aggregation`` uses on the production trace.

Usage:
    python -m src.collection.analysis.tpcds_aggregation
    python -m src.collection.analysis.tpcds_aggregation --tpcds-dir ~/prj/akanto-cldr/transport-aware-spark-model/data
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from src.collection.spark.model_parser import _dag_level_groups
from src.collection.analysis.verify_aggregation import (
    _PACKED,
    _PRINTED,
    _group_time,
    compute_metrics,
)

_DEFAULT_TPCDS = Path.home() / "prj/akanto-cldr/transport-aware-spark-model/data"

_CUMULATIVE = "Cumulative sum"
_PUBLISHED = "Published groups"
_PUBLISHED_PRINTED = "Published, printed Eq."
_DAG_PACKED = "DAG level, packed"
_DAG_PRINTED = "DAG level, printed Eq."
_ONE_GROUP = "One group (control)"

_ARMS = [_CUMULATIVE, _PUBLISHED, _PUBLISHED_PRINTED, _DAG_PACKED, _DAG_PRINTED, _ONE_GROUP]
_METRICS = ["MAE", "RMSE", "MAPE%", "WMAPE%", "R2"]


# ─── Load ────────────────────────────────────────────────────────────────────


def _config(row: dict[str, Any]) -> tuple[str, str, float, float]:
    """The (format, codec, bandwidth, latency) key a run was measured under."""
    fmt = "parquet" if row.get("file_format_parquet") else "orc"
    if row.get("compression_codec_snappy"):
        codec = "snappy"
    elif row.get("compression_codec_gzip"):
        codec = "gzip"
    else:
        codec = "uncompressed"
    return fmt, codec, float(row["tsg_max_bw_gbs"]), float(row["network_latency_ms"])


def load_runs(tpcds_dir: Path) -> list[dict[str, Any]]:
    """One record per TPC-DS run, joined across the summary files and the graphs."""
    jobs_path = tpcds_dir / "enriched_measurement_summary.json"
    stages_path = tpcds_dir / "enriched_stage_measurement_summary.json"
    if not jobs_path.is_file() or not stages_path.is_file():
        sys.exit(
            f"Error: TPC-DS data not found under {tpcds_dir}. Pass --tpcds-dir with the "
            f"path to the transport-aware-spark-model data directory."
        )

    with open(jobs_path) as f:
        jobs = json.load(f)
    with open(stages_path) as f:
        stage_rows = json.load(f)

    by_run: dict[tuple, dict[int, dict[str, float]]] = defaultdict(dict)
    for s in stage_rows:
        fmt, codec, bw, lat = _config(s)
        by_run[(s["query_id"], fmt, codec, bw, lat)][int(s["stage_id"])] = {
            "time": float(s["stage_duration_s"]),
            # The transport-aware model's predicted stage duration, carried so the
            # error-sensitivity analysis can substitute it for the observed one.
            "pred": float(s["T_pred_total"]) if s.get("T_pred_total") is not None else None,
            "num_tasks": int(s["num_tasks"]),
            "read": float(s.get("input_read_gb") or 0.0),
            "E": max(
                int(s["spark_num_executors"]) * int(s["spark_executor_cores"]), 1
            ),
        }

    graphs: dict[tuple[str, str, str], dict | None] = {}

    def graph(query: str, fmt: str, codec: str) -> dict | None:
        key = (query, fmt, codec)
        if key not in graphs:
            path = tpcds_dir / "graph" / f"{fmt}_{codec}" / f"{query}_groups.json"
            with open(path) as f:
                graphs[key] = json.load(f) if path.is_file() else None
        return graphs[key]

    runs: list[dict[str, Any]] = []
    for job in jobs:
        query = job["query_id"]
        fmt, codec, bw, lat = _config(job)
        feat = by_run.get((query, fmt, codec, bw, lat))
        g = graph(query, fmt, codec)
        if not feat or g is None:
            continue
        stage_ids = [int(x) for x in g["stages"] if int(x) in feat]
        if not stage_ids:
            continue
        # A graph can name a stage the summary has no row for. Those stages are
        # already dropped from stage_ids, so their edges have to go too, or the
        # level rule sees a parent it cannot place.
        present = set(stage_ids)
        edges = [
            (int(u), int(v))
            for u, v in g["stage_edges"]
            if int(u) in present and int(v) in present
        ]
        runs.append(
            {
                "query": query,
                "fmt": fmt,
                "codec": codec,
                "bw": bw,
                "lat": lat,
                "feat": feat,
                "stage_ids": stage_ids,
                "edges": edges,
                "published": [
                    {"members": [int(x) for x in grp["members"]]} for grp in g["groups"]
                ],
                "dag": [
                    {"members": m} for m in _dag_level_groups(stage_ids, edges)
                ],
                "E": max(next(iter(feat.values()))["E"], 1),
                "overhead": float(g.get("scheduler_overhead_s", 0.0)),
                "observed": float(job["exec_time_s"]),
            }
        )
    return runs


# ─── Arms ────────────────────────────────────────────────────────────────────


def _job_time(groups, feat, E, overhead, saturated=_PACKED) -> float:
    total = 0.0
    for g in groups:
        members = [m for m in g["members"] if m in feat]
        if members:
            total += _group_time(members, feat, E, saturated)
    return total + overhead


def arm_predictions(run: dict[str, Any]) -> dict[str, float]:
    feat, E, ov = run["feat"], run["E"], run["overhead"]
    one_group = [{"members": sorted(feat)}]
    return {
        _CUMULATIVE: sum(f["time"] for f in feat.values()) + ov,
        _PUBLISHED: _job_time(run["published"], feat, E, ov, _PACKED),
        _PUBLISHED_PRINTED: _job_time(run["published"], feat, E, ov, _PRINTED),
        _DAG_PACKED: _job_time(run["dag"], feat, E, ov, _PACKED),
        _DAG_PRINTED: _job_time(run["dag"], feat, E, ov, _PRINTED),
        _ONE_GROUP: _job_time(one_group, feat, E, ov, _PACKED),
    }


def score(runs: list[dict], arms: list[str] | None = None) -> dict[str, dict[str, float]]:
    arms = arms or _ARMS
    observed = [r["observed"] for r in runs]
    preds = {a: [] for a in arms}
    for r in runs:
        p = arm_predictions(r)
        for a in arms:
            preds[a].append(p[a])
    return {a: compute_metrics(observed, preds[a]) for a in arms}


# ─── Tables ──────────────────────────────────────────────────────────────────


def _print_arms(runs: list[dict], title: str) -> None:
    scored = score(runs)
    print(f"\n=== {title} ===")
    print(f"Number of runs: {len(runs)}")
    print(f"{'Metric':<8}" + "".join(f"{a:>24}" for a in _ARMS))
    for k in _METRICS:
        print(f"{k:<8}" + "".join(f"{scored[a][k]:>24.4f}" for a in _ARMS))


def table_saturation(runs: list[dict]) -> None:
    """How often the two saturated-case forms diverge, and by how much.

    Two counts, and they are not the same number. The saturated branch *firing* is a
    property of the group: more than one member, and pooled tasks above the budget.
    The two forms *diverging* is stricter, because they coincide whenever no member
    leaves spare slots in its final wave. Report both, or the reader cannot tell which
    one a percentage refers to.
    """
    groups_total = groups_saturated = 0
    runs_fired = 0
    ratios: list[float] = []

    for r in runs:
        feat, E = r["feat"], r["E"]
        fired = False
        for g in r["published"]:
            members = [m for m in g["members"] if m in feat]
            if not members:
                continue
            groups_total += 1
            if len(members) > 1 and sum(feat[m]["num_tasks"] for m in members) > E:
                groups_saturated += 1
                fired = True
        if fired:
            runs_fired += 1
        packed = _job_time(r["published"], feat, E, r["overhead"], _PACKED)
        printed = _job_time(r["published"], feat, E, r["overhead"], _PRINTED)
        if packed > 0 and printed > packed:
            ratios.append(printed / packed - 1.0)

    arr = np.array(ratios)
    print("\n=== Saturated case: how often the two forms diverge ===")
    print(f"  Groups where the saturated branch fires: {groups_saturated} of "
          f"{groups_total} ({groups_saturated / groups_total * 100:.1f}%)")
    print(f"  Runs with at least one such group: {runs_fired} of {len(runs)} "
          f"({runs_fired / len(runs) * 100:.1f}%)")
    print(f"  Runs where the two forms actually differ: {len(ratios)} of {len(runs)} "
          f"({len(ratios) / len(runs) * 100:.1f}%)")
    print(f"  On those runs the printed form over-predicts the packed form by: "
          f"median {np.median(arr) * 100:.1f}%, mean {arr.mean() * 100:.1f}%, "
          f"max {arr.max() * 100:.1f}%")


def table_signed_error(runs: list[dict]) -> None:
    """Bias direction for both saturated-case forms, on the published groups."""
    observed = np.array([r["observed"] for r in runs], dtype=float)
    print("\n=== Signed error on the published groups ===")
    print(f"{'Form':<28}{'mean':>12}{'median':>12}{'over-predicts':>16}")
    for label, sat in [("Wave packed", _PACKED), ("Stage-level sum", _PRINTED)]:
        pred = np.array(
            [_job_time(r["published"], r["feat"], r["E"], r["overhead"], sat)
             for r in runs], dtype=float)
        err = pred - observed
        print(f"{label:<28}{err.mean():>+11.3f}s{np.median(err):>+11.3f}s"
              f"{(err > 0).mean() * 100:>15.1f}%")


def table_sparsity(runs: list[dict], production_dir: Path | None) -> None:
    """Stage-graph density, TPC-DS against production. The sparsity argument."""
    print("\n=== Stage-graph sparsity ===")
    print(f"{'Dataset':<32}{'median stages':>15}{'median edges':>14}"
          f"{'no edges':>22}{'edges/stage':>13}")

    def row(label: str, per_job: list[tuple[int, int]]) -> None:
        stages = np.array([s for s, _ in per_job])
        edges = np.array([e for _, e in per_job])
        zero = int((edges == 0).sum())
        share = f"{zero} of {len(per_job)} ({zero / len(per_job) * 100:.1f}%)"
        print(f"{label:<32}{np.median(stages):>15.0f}{np.median(edges):>14.0f}"
              f"{share:>22}{edges.sum() / stages.sum():>13.3f}")

    seen: set[tuple[str, str, str]] = set()
    per_job: list[tuple[int, int]] = []
    for r in runs:
        if r["fmt"] == "parquet" and r["codec"] == "snappy":
            key = (r["query"], r["fmt"], r["codec"])
            if key not in seen:
                seen.add(key)
                per_job.append((len(r["stage_ids"]), len(r["edges"])))
    if per_job:
        row("TPC-DS (parquet_snappy)", per_job)

    if production_dir is not None:
        path = production_dir / "graph_features.json"
        if path.is_file():
            with open(path) as f:
                graphs = json.load(f)
            row("Production trace",
                [(len(g["stages"]), len(g["stage_edges"])) for g in graphs])
        else:
            print(f"  (production graphs not found at {path}, row skipped)")


def table_by_config(runs: list[dict], title: str, min_n: int = 20) -> None:
    """The arms re-scored inside each interconnect configuration."""
    cols = [_CUMULATIVE, _DAG_PACKED, _ONE_GROUP]
    short = {_CUMULATIVE: "cum", _DAG_PACKED: "DAG", _ONE_GROUP: "ctl"}
    print(f"\n=== {title} ===")
    print(f"  n = {len(runs)}")
    print(f"{'B_link':>8}{'RTT':>8}{'n':>7}"
          + "".join(f"{short[a] + ' R2':>11}" for a in cols)
          + "".join(f"{short[a] + ' MAE':>12}" for a in cols))
    for bw, lat in sorted({(r["bw"], r["lat"]) for r in runs}):
        sel = [r for r in runs if r["bw"] == bw and r["lat"] == lat]
        if len(sel) < min_n:
            continue
        m = score(sel, cols)
        print(f"{bw:>8.0f}{lat:>8.2f}{len(sel):>7}"
              + "".join(f"{m[a]['R2']:>11.4f}" for a in cols)
              + "".join(f"{m[a]['MAE']:>12.3f}" for a in cols))


def table_transfer_share(runs: list[dict]) -> list[dict]:
    """Transfer-stage share of stage time. Returns the transfer-dominated subset."""
    for r in runs:
        total = sum(f["time"] for f in r["feat"].values())
        xfer = sum(f["time"] for f in r["feat"].values() if f["read"] > 0.0)
        r["share"] = xfer / total if total > 0 else 0.0

    shares = np.array([r["share"] for r in runs])
    print("\n=== Transfer-stage share of stage time, per run ===")
    for q in (10, 25, 50, 75, 90):
        print(f"  p{q:<3} = {np.percentile(shares, q) * 100:5.1f}%")
    print(f"  mean  = {shares.mean() * 100:5.1f}%")
    print("\n  By configuration:")
    print(f"{'B_link':>8}{'RTT':>8}{'n':>7}{'mean share':>14}")
    for bw, lat in sorted({(r["bw"], r["lat"]) for r in runs}):
        sel = [r["share"] for r in runs if r["bw"] == bw and r["lat"] == lat]
        print(f"{bw:>8.0f}{lat:>8.2f}{len(sel):>7}{np.mean(sel) * 100:>13.1f}%")

    return [r for r in runs if r["share"] >= 0.75]


# ─── Main ────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Re-derive the TPC-DS aggregation tables from committed code")
    parser.add_argument("--tpcds-dir", type=Path, default=_DEFAULT_TPCDS,
                        help="The transport-aware-spark-model data directory (read only)")
    parser.add_argument("--production-dir", type=Path,
                        default=Path("data/model_features/spark"),
                        help="Production model features, for the sparsity comparison")
    args = parser.parse_args()

    runs = load_runs(args.tpcds_dir)
    if not runs:
        sys.exit("Error: no TPC-DS runs joined. Check --tpcds-dir.")
    print(f"TPC-DS runs joined: {len(runs)}")

    _print_arms(runs, "1. The arms on TPC-DS")
    table_saturation(runs)
    table_signed_error(runs)
    table_sparsity(runs, args.production_dir if args.production_dir.is_dir() else None)
    table_by_config(runs, "5. The arms inside each interconnect configuration")
    dominated = table_transfer_share(runs)
    table_by_config(
        dominated,
        "6. Dilution control: runs where transfer stages carry 75% or more of stage time",
    )


if __name__ == "__main__":
    main()
