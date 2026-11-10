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

"""Verify how well the group graphs reconstruct the observed job time.

This command is a check, not a predictor. It reads the model features that
``make model-features`` writes and asks one question: given the real per-stage
times, how close does each aggregation method come to the real job time?

    data/model_features/spark/job_features.json     the observed job time
    data/model_features/spark/stage_features.json   the observed stage times
    data/model_features/spark/graph_features.json   the two group graphs

The three files join on ``query_id``. Two choices are independent here: how the
groups are formed, and how a group is timed when its tasks exceed the core budget.
The arms cover the combinations that matter:

  - Cumulative sum:      every stage time added up. The baseline, after Mustafa
                         et al., and the arm the paper compares against.
  - DAG, printed Eq.:    the ``dag_groups`` times added up, with the saturated case
                         exactly as Eq. (access.tex:514-517) prints it, the sum of
                         the member stage times. This is the paper as written.
  - DAG, wave packed:    the same groups, but the saturated case as the model code
                         computes it. This is the paper as implemented.
  - Observed, packed:    the ``observed_groups`` times, wave packed. The groups come
                         from wall-clock overlap, so this arm reads the answer. It
                         is a ceiling, not a predictor.
  - One group (control): every stage placed in a single group, which assumes total
                         concurrency and ignores the DAG. It separates what the
                         dependency structure buys from what the core-budget wave
                         packing buys on its own.

Every arm adds ``scheduler_overhead_s``. Note that the paper has no scheduler
overhead term: its Eq. (access.tex:519-521) is purely the sum of the group
durations. The addend is carried here because the model code adds it.

The grouping rule behind ``dag_groups`` is the paper's own definition of a group
(access.tex:502). The wave-packed group time is the published model's aggregation
(``transport_aware_model/model/predict_graph_groups.py``), used without change. The
only change is the input - the real ``stage_duration_s`` stands in for the model's
predicted stage time, because this check measures the aggregation and the graph,
not a network model.

It prints two tables. The first is the global result. The second breaks the same
metrics down by observed job runtime. Nothing is written to disk.

Usage:
    python -m src.collection.analysis.verify_aggregation
    python -m src.collection.analysis.verify_aggregation --input data/model_features/spark
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np

# The observed runtime bins for the second table (seconds). Same edges the model
# uses for its residual plots.
_BIN_EDGES = [0.3, 10.0, 30.0, 100.0, float("inf")]
_BIN_LABELS = ["0.3–10", "10–30", "30–100", "100+"]

_CUMULATIVE = "Cumulative sum"
_DAG_PAPER = "DAG, printed Eq."
_DAG_PACKED = "DAG, wave packed"
_OBSERVED = "Observed, packed"
_ONE_GROUP = "One group (control)"

# The print order. The cumulative arm is the baseline, so it comes first.
_ARMS = [_CUMULATIVE, _DAG_PAPER, _DAG_PACKED, _OBSERVED, _ONE_GROUP]

# How a group is timed when its tasks exceed the core budget. The two variants
# differ only in this saturated case; a lone stage and a group that fits the
# budget are the same either way.
_PRINTED = "printed"   # T_group = sum of the member stage times (Eq. access.tex:514-517)
_PACKED = "packed"     # pool the members' tasks into waves of E, longest first (the code)

_METRICS = ["MAE", "RMSE", "MAPE%", "WMAPE%", "R2"]


# ─── Metrics (verbatim from the published model's compute_metrics) ───────────


def compute_metrics(y_true: list[float], y_pred: list[float]) -> dict[str, float]:
    """MAE, RMSE, MAPE%, WMAPE%, and R2, exactly as the model computes them."""
    y_true_arr = np.asarray(y_true, dtype=float)
    y_pred_arr = np.asarray(y_pred, dtype=float)
    err = y_pred_arr - y_true_arr

    mae = float(np.mean(np.abs(err)))
    rmse = float(math.sqrt(np.mean(err**2)))

    nonzero = y_true_arr != 0
    mape = (
        float(np.mean(np.abs(err[nonzero] / y_true_arr[nonzero])) * 100.0)
        if np.any(nonzero)
        else float("nan")
    )

    denom = float(np.sum(np.abs(y_true_arr)))
    wmape = float(np.sum(np.abs(err)) / denom * 100.0) if denom > 0 else float("nan")

    ss_res = float(np.sum(err**2))
    ss_tot = float(np.sum((y_true_arr - float(np.mean(y_true_arr))) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    return {"MAE": mae, "RMSE": rmse, "MAPE%": mape, "WMAPE%": wmape, "R2": r2}


# ─── Aggregation (the model's group_time / resource_filling_time, verbatim) ──


def _resource_filling_time(
    members: list[int], feat: dict[int, dict[str, float]], E: int
) -> float:
    """Wave-packed time for a parallel group whose tasks exceed the core budget.

    Each stage's tasks get an even share of the stage time; the tasks are packed
    into waves of ``E`` (longest first). This mirrors the model exactly, with the
    real stage time in place of the predicted one.
    """
    task_durations: list[float] = []
    for sid in members:
        num_tasks = int(feat[sid]["num_tasks"])
        waves = max(1, math.ceil(num_tasks / E))
        tau = float(feat[sid]["time"]) / waves
        task_durations.extend([tau] * num_tasks)

    n = len(task_durations)
    if n == 0:
        return 0.0
    task_durations.sort(reverse=True)

    if n <= E:
        return float(task_durations[0])

    t = 0.0
    for i in range(0, n, E):
        t += float(task_durations[i])
    return float(t)


def _group_time(
    members: list[int],
    feat: dict[int, dict[str, float]],
    E: int,
    saturated: str = _PACKED,
) -> float:
    """The time of one group.

    A lone stage is its own time, and a group whose tasks fit the core budget is
    the longest member. Those two cases are Eq. (access.tex:505-507) and Eq.
    (access.tex:509-512), and they do not depend on ``saturated``.

    The saturated case has two published forms. ``_PRINTED`` is Eq.
    (access.tex:514-517), the sum of the member stage times. ``_PACKED`` is what
    the model code computes: the members' tasks are pooled and packed into waves
    of ``E``, longest first.
    """
    if len(members) == 1:
        return float(feat[members[0]]["time"])

    n_tasks = sum(int(feat[sid]["num_tasks"]) for sid in members)
    if n_tasks <= E:
        return float(max(float(feat[sid]["time"]) for sid in members))

    if saturated == _PRINTED:
        return float(sum(float(feat[sid]["time"]) for sid in members))

    return _resource_filling_time(members, feat, E)


def _grouping_job_s(
    groups: list[dict[str, Any]],
    feat: dict[int, dict[str, float]],
    E: int,
    overhead: float,
    saturated: str = _PACKED,
) -> float:
    """A grouped job time: the group times summed, plus the overhead.

    The groups are summed, so their order does not matter (the model serializes
    them in topological order, but a sum is order-independent).
    """
    total = 0.0
    for g in groups:
        members = [int(x) for x in g["members"]]
        total += _group_time(members, feat, E, saturated)
    return total + overhead


def _cumulative_job_s(feat: dict[int, dict[str, float]], overhead: float) -> float:
    """The simple job time: every stage time summed, plus the overhead."""
    return sum(float(f["time"]) for f in feat.values()) + overhead


# ─── Load and join ───────────────────────────────────────────────────────────


def _load(input_dir: Path) -> tuple[list[dict], list[dict], list[dict]]:
    def _read(name: str) -> list[dict]:
        path = input_dir / name
        if not path.is_file():
            sys.exit(
                f"Error: {path} not found. Run 'make model-features' first."
            )
        with open(path) as f:
            return json.load(f)

    return (
        _read("job_features.json"),
        _read("stage_features.json"),
        _read("graph_features.json"),
    )


def evaluate(
    jobs: list[dict], stages: list[dict], graphs: list[dict]
) -> tuple[list[float], dict[str, list[float]], int]:
    """Compute the observed time and every arm's time for each run.

    Returns the observed times, a dict of arm name to its parallel list of
    predictions, and the count of runs skipped because a graph named a stage with
    no stage record.
    """
    stages_by_id: dict[str, dict[int, dict[str, float]]] = {}
    for s in stages:
        qid = s["query_id"]
        stages_by_id.setdefault(qid, {})[int(s["stage_id"])] = {
            "time": float(s["stage_duration_s"]),
            "num_tasks": int(s["num_tasks"]),
        }
    graph_by_id = {g["query_id"]: g for g in graphs}

    observed: list[float] = []
    preds: dict[str, list[float]] = {name: [] for name in _ARMS}
    skipped = 0

    for job in jobs:
        qid = job["query_id"]
        graph = graph_by_id.get(qid)
        if graph is None:
            continue
        feat = stages_by_id.get(qid, {})

        dag_groups = graph.get("dag_groups")
        obs_groups = graph.get("observed_groups", graph.get("groups", []))
        if dag_groups is None:
            sys.exit(
                "Error: graph_features.json has no 'dag_groups'. Rerun "
                "'make model-features' to write both group graphs."
            )

        # Every group graph must only name stages we have a time for.
        members = [
            int(sid)
            for groups in (dag_groups, obs_groups)
            for g in groups
            for sid in g["members"]
        ]
        if any(sid not in feat for sid in members):
            skipped += 1
            continue

        E = int(job.get("spark_num_executors") or 0) * int(job.get("spark_executor_cores") or 0)
        if E < 1:
            E = 1
        overhead = float(graph.get("scheduler_overhead_s", 0.0))
        one_group = [{"members": sorted(feat)}] if feat else []

        observed.append(float(job["exec_time_s"]))
        preds[_CUMULATIVE].append(_cumulative_job_s(feat, overhead))
        preds[_DAG_PAPER].append(_grouping_job_s(dag_groups, feat, E, overhead, _PRINTED))
        preds[_DAG_PACKED].append(_grouping_job_s(dag_groups, feat, E, overhead, _PACKED))
        preds[_OBSERVED].append(_grouping_job_s(obs_groups, feat, E, overhead, _PACKED))
        preds[_ONE_GROUP].append(_grouping_job_s(one_group, feat, E, overhead, _PACKED))

    return observed, preds, skipped


# ─── Print ───────────────────────────────────────────────────────────────────


def _print_metric_table(
    observed: list[float], preds: dict[str, list[float]], title: str
) -> None:
    """One table: a row per metric, a column per arm, and the best arm."""
    scored = {name: compute_metrics(observed, preds[name]) for name in _ARMS}

    print(f"\n=== {title} ===")
    print(f"Number of jobs: {len(observed)}")
    print(f"{'Metric':<8}" + "".join(f"{name:>21}" for name in _ARMS) + f"{'Best':>21}")

    for k in _METRICS:
        values = {name: scored[name][k] for name in _ARMS}
        usable = {n: v for n, v in values.items() if not math.isnan(v)}
        if not usable:
            best = "—"
        elif k == "R2":
            best = max(usable, key=lambda n: usable[n])
        else:
            best = min(usable, key=lambda n: usable[n])
        print(f"{k:<8}" + "".join(f"{values[n]:>21.4f}" for n in _ARMS) + f"{best:>21}")


def _bin_index(value: float) -> int | None:
    """The runtime bin for an observed time, or None when it is out of range."""
    for i in range(len(_BIN_LABELS)):
        if _BIN_EDGES[i] <= value < _BIN_EDGES[i + 1]:
            return i
    return None


def print_report(
    observed: list[float], preds: dict[str, list[float]], skipped: int
) -> None:
    if not observed:
        print("No jobs could be evaluated (no graph and stage records joined).")
        return

    _print_metric_table(observed, preds, "Global (all jobs)")

    print("\n=== By observed runtime [s] ===")
    for i, label in enumerate(_BIN_LABELS):
        keep = [j for j, o in enumerate(observed) if _bin_index(o) == i]
        if not keep:
            continue
        obs_b = [observed[j] for j in keep]
        preds_b = {name: [preds[name][j] for j in keep] for name in _ARMS}
        _print_metric_table(obs_b, preds_b, f"{label} s  (n={len(obs_b)})")

    if skipped:
        print(f"\nNote: skipped {skipped} run(s) whose graph named a stage with no "
              f"stage record.", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify how well the group graphs reconstruct observed job time")
    parser.add_argument("--input", type=Path, default=Path("data/model_features/spark"),
                        help="Directory holding the model feature JSON files")
    args = parser.parse_args()

    if not args.input.is_dir():
        sys.exit(f"Error: input directory not found: {args.input}")

    jobs, stages, graphs = _load(args.input)
    observed, preds, skipped = evaluate(jobs, stages, graphs)
    print_report(observed, preds, skipped)


if __name__ == "__main__":
    main()
