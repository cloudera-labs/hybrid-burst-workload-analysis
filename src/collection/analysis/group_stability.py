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

"""Is a job's concurrency structure a property of the query, or of the single run?

``verify_aggregation`` shows that grouping stages by observed wall-clock overlap
beats grouping them by DAG dependency level. That arm is not a predictor: it reads
the timeline of the very run it is scoring.

Shah et al. (CNSM 2019) do not treat it as a ceiling. PERIDOT reads the same
structure from a *reference execution* and assumes it carries over to the next run
of the same application. This module tests that assumption directly.

    For each family of repeated executions, take the groups that run A produced and
    score a later run B with them, using B's own stage times and B's own core budget.

If the structure transfers, a predictor that already stores historical profiles of
recurring queries gets the overlap grouping for free: no target-side execution and
no new collection. If it does not transfer, the instability is itself the answer.

Families are keyed on (app_name, stage count). Equal stage count is a cheap guard
that the plan did not change between the two runs. Runs are ordered by the sequence
number in their YARN application id, so a donor always precedes its recipient; a
random pairing would leak information that a deployed predictor cannot have.

Two donor policies are reported:

  - previous:  the most recent earlier run in the family. What a system holding the
               latest profile would use.
  - first:     the earliest run in the family, held fixed. Tests whether the
               structure survives drift over the whole collection window.

The arms are those of ``verify_aggregation``, restricted to the runs that have a
donor so that every arm is scored on the same set:

  - Cumulative sum       the baseline, after Mustafa et al.
  - DAG, wave packed     the structural baseline. Computable before the run
  - Own-run overlap      the ceiling. Reads the run it scores, so not a predictor
  - Transferred overlap  the arm under test. Another run's groups, this run's times
  - One group (control)  the DAG discarded entirely

Usage:
    python -m src.collection.analysis.group_stability
    python -m src.collection.analysis.group_stability --input data/model_features/spark
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from src.collection.analysis.verify_aggregation import (
    _PACKED,
    _grouping_job_s,
    _cumulative_job_s,
    compute_metrics,
)

_CUMULATIVE = "Cumulative sum"
_DAG = "DAG, wave packed"
_OWN = "Own-run overlap"
_TRANSFER = "Transferred overlap"
_ONE_GROUP = "One group (control)"

_ARMS = [_CUMULATIVE, _DAG, _OWN, _TRANSFER, _ONE_GROUP]
_METRICS = ["MAE", "RMSE", "MAPE%", "WMAPE%", "R2"]


# ─── Run ordering ────────────────────────────────────────────────────────────


def _app_order(app_id: str) -> tuple[int, int]:
    """Chronological sort key from a YARN application id.

    ``application_<cluster_epoch>_<sequence>``. The sequence number increases with
    submission order inside one cluster session, so this orders runs in time without
    needing a submission timestamp, which the feature files do not carry.
    """
    parts = str(app_id).split("_")
    try:
        return int(parts[-2]), int(parts[-1])
    except (ValueError, IndexError):
        return 0, 0


# ─── Group transfer ──────────────────────────────────────────────────────────


def _transfer(donor_groups: list[dict[str, Any]], stage_ids: set[int]) -> list[dict]:
    """Re-express the donor's grouping over the recipient's stages.

    Each donor group contributes the members the recipient also has. A recipient
    stage the donor never saw becomes its own singleton, which is the conservative
    choice: it charges that stage in full rather than hiding it behind another.
    """
    groups: list[dict[str, Any]] = []
    covered: set[int] = set()
    for g in donor_groups:
        members = sorted(int(s) for s in g["members"] if int(s) in stage_ids)
        if members:
            groups.append({"members": members})
            covered.update(members)
    for sid in sorted(stage_ids - covered):
        groups.append({"members": [sid]})
    return groups


def _partition(groups: list[dict]) -> frozenset[frozenset[int]]:
    """A grouping as a comparable set of sets, so two partitions can be equated."""
    return frozenset(frozenset(int(s) for s in g["members"]) for g in groups)


def _descendants(stage_ids: set[int], edges: list[list[int]]) -> dict[int, set[int]]:
    """Transitive descendants per stage, for the antichain check below."""
    children: dict[int, list[int]] = defaultdict(list)
    for u, v in edges:
        u, v = int(u), int(v)
        if u in stage_ids and v in stage_ids:
            children[u].append(v)

    desc: dict[int, set[int]] = {}

    def walk(node: int) -> set[int]:
        if node in desc:
            return desc[node]
        desc[node] = set()
        out: set[int] = set()
        for c in children[node]:
            out.add(c)
            out |= walk(c)
        desc[node] = out
        return out

    for sid in stage_ids:
        walk(sid)
    return desc


def _breaks_antichain(groups: list[dict], desc: dict[int, set[int]]) -> bool:
    """True when a transferred group holds a stage and one of its own descendants.

    ``_overlap_groups`` guarantees this never happens inside the run it was built
    from. A grouping carried over from another run carries no such guarantee, and a
    group containing a dependency pair asserts that dependent stages ran at the same
    time. Counting these is part of the result, not a detail.
    """
    for g in groups:
        members = [int(s) for s in g["members"]]
        for a in members:
            if desc.get(a, set()) & set(members):
                return True
    return False


# ─── Evaluate ────────────────────────────────────────────────────────────────


def _load(input_dir: Path) -> tuple[list[dict], list[dict], list[dict]]:
    def read(name: str) -> list[dict]:
        path = input_dir / name
        if not path.is_file():
            sys.exit(f"Error: {path} not found. Run 'make model-features' first.")
        with open(path) as f:
            return json.load(f)

    return (
        read("job_features.json"),
        read("stage_features.json"),
        read("graph_features.json"),
    )


def build_runs(
    jobs: list[dict], stages: list[dict], graphs: list[dict]
) -> list[dict[str, Any]]:
    """One record per run, carrying everything the arms need."""
    stages_by_id: dict[str, dict[int, dict[str, float]]] = {}
    for s in stages:
        stages_by_id.setdefault(s["query_id"], {})[int(s["stage_id"])] = {
            "time": float(s["stage_duration_s"]),
            "num_tasks": int(s["num_tasks"]),
        }
    graph_by_id = {g["query_id"]: g for g in graphs}

    runs: list[dict[str, Any]] = []
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
        members = [
            int(sid)
            for gs in (dag_groups, obs_groups)
            for g in gs
            for sid in g["members"]
        ]
        if not feat or any(sid not in feat for sid in members):
            continue

        E = int(job.get("spark_num_executors") or 0) * int(
            job.get("spark_executor_cores") or 0
        )
        runs.append(
            {
                "app_id": job["app_id"],
                "name": job.get("app_name") or graph.get("name"),
                "order": _app_order(job["app_id"]),
                "feat": feat,
                "stage_ids": set(feat),
                "edges": graph.get("stage_edges", []),
                "dag_groups": dag_groups,
                "obs_groups": obs_groups,
                "E": max(E, 1),
                "overhead": float(graph.get("scheduler_overhead_s", 0.0)),
                "observed": float(job["exec_time_s"]),
            }
        )
    return runs


def families(runs: list[dict]) -> dict[tuple[str, int], list[dict]]:
    """Group runs by (app_name, stage count), each family ordered in time.

    Equal stage count is the plan-stability guard. Two executions of the same
    application that produced a different number of stages are not comparable, and
    transferring a grouping between them would be a different experiment.
    """
    fams: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in runs:
        fams[(r["name"], len(r["stage_ids"]))].append(r)
    for members in fams.values():
        members.sort(key=lambda r: r["order"])
    return {k: v for k, v in fams.items() if len(v) > 1}


def evaluate(runs: list[dict], policy: str) -> dict[str, Any]:
    """Score every arm on the runs that have a donor under ``policy``."""
    observed: list[float] = []
    preds: dict[str, list[float]] = {a: [] for a in _ARMS}
    rows: list[dict[str, Any]] = []
    id_mismatch = 0
    antichain_broken = 0

    for members in families(runs).values():
        for i in range(1, len(members)):
            donor = members[0] if policy == "first" else members[i - 1]
            recip = members[i]

            if donor["stage_ids"] != recip["stage_ids"]:
                id_mismatch += 1

            transferred = _transfer(donor["obs_groups"], recip["stage_ids"])
            desc = _descendants(recip["stage_ids"], recip["edges"])
            if _breaks_antichain(transferred, desc):
                antichain_broken += 1

            feat, E, ov = recip["feat"], recip["E"], recip["overhead"]
            one_group = [{"members": sorted(feat)}]

            row = {
                "observed": recip["observed"],
                "n_stages": len(feat),
                "n_multi": sum(1 for g in recip["obs_groups"] if len(g["members"]) > 1),
                "n_obs_groups": len(recip["obs_groups"]),
                "n_dag_groups": len(recip["dag_groups"]),
                "identical": _partition(transferred) == _partition(recip["obs_groups"]),
                _CUMULATIVE: _cumulative_job_s(feat, ov),
                _DAG: _grouping_job_s(recip["dag_groups"], feat, E, ov, _PACKED),
                _OWN: _grouping_job_s(recip["obs_groups"], feat, E, ov, _PACKED),
                _TRANSFER: _grouping_job_s(transferred, feat, E, ov, _PACKED),
                _ONE_GROUP: _grouping_job_s(one_group, feat, E, ov, _PACKED),
            }
            rows.append(row)
            observed.append(row["observed"])
            for arm in _ARMS:
                preds[arm].append(row[arm])

    return {
        "observed": observed,
        "preds": preds,
        "rows": rows,
        "id_mismatch": id_mismatch,
        "antichain_broken": antichain_broken,
    }


# ─── Print ───────────────────────────────────────────────────────────────────


def _print_table(observed: list[float], preds: dict[str, list[float]], title: str) -> None:
    scored = {a: compute_metrics(observed, preds[a]) for a in _ARMS}
    print(f"\n=== {title} ===")
    print(f"Number of scored runs: {len(observed)}")
    print(f"{'Metric':<8}" + "".join(f"{a:>21}" for a in _ARMS))
    for k in _METRICS:
        print(f"{k:<8}" + "".join(f"{scored[a][k]:>21.4f}" for a in _ARMS))


def _print_gate(observed: list[float], preds: dict[str, list[float]]) -> None:
    """The pass criterion, stated as a fraction of the gap the transfer recovers."""
    mae = {a: compute_metrics(observed, preds[a])["MAE"] for a in _ARMS}
    r2 = {a: compute_metrics(observed, preds[a])["R2"] for a in _ARMS}
    gap = mae[_DAG] - mae[_OWN]
    recovered = (mae[_DAG] - mae[_TRANSFER]) / gap if gap > 0 else float("nan")

    print("\n=== Gate 2 ===")
    print(f"  DAG (structural floor)  MAE {mae[_DAG]:8.4f}   R2 {r2[_DAG]:7.4f}")
    print(f"  Own-run overlap (ceil)  MAE {mae[_OWN]:8.4f}   R2 {r2[_OWN]:7.4f}")
    print(f"  Transferred overlap     MAE {mae[_TRANSFER]:8.4f}   R2 {r2[_TRANSFER]:7.4f}")
    print(f"  Gap available: {gap:.4f} s MAE.  Recovered by transfer: {recovered * 100:.1f}%")


def _slice(rows: list[dict], label: str) -> None:
    """One subset row: is the transfer result carried by an easy majority?"""
    if len(rows) < 30:
        return
    observed = [r["observed"] for r in rows]
    m = {a: compute_metrics(observed, [r[a] for r in rows]) for a in _ARMS}
    gap = m[_DAG]["MAE"] - m[_OWN]["MAE"]
    recovered = (m[_DAG]["MAE"] - m[_TRANSFER]["MAE"]) / gap * 100 if gap > 0 else float("nan")
    print(f"{label:<40}{len(rows):>7}{m[_DAG]['MAE']:>11.3f}{m[_OWN]['MAE']:>11.3f}"
          f"{m[_TRANSFER]['MAE']:>13.3f}{recovered:>13.1f}%")


def print_slices(rows: list[dict]) -> None:
    """The transfer result broken down, so a 99% recovery can be checked."""
    print("\n=== Transfer by subset ===")
    print(f"{'Subset':<40}{'n':>7}{'DAG MAE':>11}{'own MAE':>11}"
          f"{'transf MAE':>13}{'recovered':>14}")
    _slice(rows, "All scored pairs")
    _slice([r for r in rows if r["n_multi"] > 0], "Runs with a multi-stage group")
    _slice([r for r in rows if r["n_multi"] >= 3], "Runs with 3 or more multi-stage groups")
    _slice([r for r in rows if r["n_stages"] >= 10], "Runs with 10 or more stages")
    _slice([r for r in rows if 30.0 <= r["observed"] < 100.0], "30-100 s band")
    _slice([r for r in rows if r["observed"] >= 100.0], "100+ s band")

    identical = sum(1 for r in rows if r["identical"])
    print(f"\n  Transferred partition identical to the recipient's own: "
          f"{identical} of {len(rows)} ({identical / len(rows) * 100:.1f}%)")


def print_mechanism_split(rows: list[dict]) -> None:
    """What the temporal grouping actually contributes, split on found concurrency.

    The natural reading is that it finds parallelism the DAG missed. It does not: on
    the runs where it finds no concurrency at all it *is* cumulative summation, and
    the DAG arm is the one that is wrong, because with no stage edges the level rule
    declares every stage concurrent.
    """
    print("\n=== What the temporal grouping contributes ===")
    print(f"{'Subset':<44}{'n':>7}{'cum MAE':>11}{'DAG MAE':>11}{'own MAE':>11}")
    for label, sel in [
        ("Overlap grouping is all singletons",
         [r for r in rows if r["n_multi"] == 0]),
        ("Overlap grouping has a multi-stage group",
         [r for r in rows if r["n_multi"] > 0]),
    ]:
        if len(sel) < 30:
            continue
        observed = [r["observed"] for r in sel]
        m = {a: compute_metrics(observed, [r[a] for r in sel]) for a in _ARMS}
        print(f"{label:<44}{len(sel):>7}{m[_CUMULATIVE]['MAE']:>11.3f}"
              f"{m[_DAG]['MAE']:>11.3f}{m[_OWN]['MAE']:>11.3f}")

    multi = [r for r in rows if r["n_stages"] > 1]
    if multi:
        import statistics
        print(f"\n  On multi-stage runs (n={len(multi)}): median groups, "
              f"DAG {statistics.median(r['n_dag_groups'] for r in multi):.0f} against "
              f"overlap {statistics.median(r['n_obs_groups'] for r in multi):.0f}, "
              f"over a median of {statistics.median(r['n_stages'] for r in multi):.0f} stages")
        print(f"  Mean groups per stage: DAG "
              f"{statistics.mean(r['n_dag_groups'] / r['n_stages'] for r in multi):.3f} "
              f"against overlap "
              f"{statistics.mean(r['n_obs_groups'] / r['n_stages'] for r in multi):.3f}")


def print_report(result: dict[str, Any], policy: str, detail: bool = False) -> None:
    observed, preds = result["observed"], result["preds"]
    if not observed:
        print("No repeated executions found, nothing to score.")
        return
    _print_table(observed, preds, f"Donor policy: {policy}")
    _print_gate(observed, preds)
    print(f"\n  Pairs whose stage id sets differ: {result['id_mismatch']}")
    print(f"  Transferred groupings breaking the antichain invariant: "
          f"{result['antichain_broken']}")
    if detail:
        print_slices(result["rows"])
        print_mechanism_split(result["rows"])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Test whether stage concurrency structure transfers between runs")
    parser.add_argument("--input", type=Path, default=Path("data/model_features/spark"),
                        help="Directory holding the model feature JSON files")
    args = parser.parse_args()

    if not args.input.is_dir():
        sys.exit(f"Error: input directory not found: {args.input}")

    jobs, stages, graphs = _load(args.input)
    runs = build_runs(jobs, stages, graphs)
    fams = families(runs)
    print(f"Runs joined: {len(runs)}")
    print(f"Families of repeated executions: {len(fams)}")
    print(f"Runs inside a family: {sum(len(v) for v in fams.values())}")

    for policy in ("previous", "first"):
        # The subset and mechanism tables are reported once, under the policy a
        # deployed predictor would use.
        print_report(evaluate(runs, policy), policy, detail=(policy == "previous"))


if __name__ == "__main__":
    main()
