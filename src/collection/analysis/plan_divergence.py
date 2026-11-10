"""How far does the executed plan drift from the plan that existed at submission time?

A submission-time predictor sees one thing: the plan Catalyst produced before the
query ran, carried by ``SparkListenerSQLExecutionStart``. Adaptive Query Execution
then rewrites that plan after each shuffle materialises, and every rewrite is
re-reported by ``SparkListenerSQLAdaptiveExecutionUpdate``. The gap between the
first plan and the last one is the ceiling on any predictor that runs before the
query does, and it is a property of the workload, not of the predictor.

This module measures that gap. It does not predict anything.

**The quantity is the shuffle count, and nothing else.** The executed plan carries
``ShuffleQueryStage``, ``BroadcastQueryStage``, ``InputAdapter``, ``ColumnarToRow``
and ``WholeStageCodegen`` nodes that the submitted plan cannot have, because they
are what materialisation and code generation add. Total node counts therefore
differ for essentially every query and measure nothing. The count of ``Exchange``
nodes is comparable across the two plans, and it is the quantity that matters: an
``Exchange`` is a shuffle, so it is where the DAGScheduler cuts the stage graph.

Three node names look like exchanges and are deliberately excluded:

- ``BroadcastExchange`` is a broadcast, which runs as its own job rather than as a
  stage of this one.
- ``ReusedExchange`` points at an exchange that another part of the plan already
  computed. It adds no stage.
- ``ShuffleQueryStage`` wraps an ``Exchange``, so its child is counted and the
  wrapper is not. Counting both would double every materialised shuffle.

**What the broadcast count does NOT measure, recorded so it is not misread.** The
count of ``BroadcastExchange`` nodes rises between the submitted and the executed
plan for most queries, and it is tempting to report that as AQE converting sort
merge joins to broadcast joins. It is mostly not. A dynamic partition pruning
subquery appears as ``SubqueryAdaptiveBroadcast`` in the submitted plan and becomes
``SubqueryBroadcast`` plus a real ``BroadcastExchange`` once it runs, which is
materialisation of something the submitted plan already contained. The broadcast
delta is printed for completeness and labelled as such. A join strategy change
shows up in the shuffle count, which is why that is the headline quantity.

**One run per cell cannot separate a regime effect from nondeterminism.** When the
same query's executed shuffle count differs between two cells, this module reports
the difference and refuses to call it a transport effect. AQE decides on statistics
that arrive when a shuffle materialises, so the decision is timing-dependent, and a
single run per cell leaves run-to-run variation and the regime confounded. Section 3
prints the per-cell values so the shape is visible, and says what would settle it:
repeat the affected queries at the cells on either side.

Usage:
    # a whole campaign: every cell that has an event log
    python -m src.collection.analysis.plan_divergence --results <campaign results dir>

    # one run
    python -m src.collection.analysis.plan_divergence --log <run>/raw/spark/app.log.zip \
        --sidecar <run>/sidecar.json --label onprem-baseline

    # with the per-query rows written out
    python -m src.collection.analysis.plan_divergence --results <dir> --output drift.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from src.collection.spark.run_splitter import Sidecar

_SQL_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
_SQL_ADAPTIVE = "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"
_JOB_START = "SparkListenerJobStart"

# The plan text's own section headers. A submitted plan that carries all four is a
# complete record of what the optimiser did; one that carries fewer is not, and a
# feature extractor built on it would be reading a different thing per run.
_PLAN_SECTION = re.compile(r"^== (.+) ==$", re.MULTILINE)

# Cardinality annotations. Present only when the plan was rendered in cost mode, and
# their absence is a real limitation rather than a detail: processed volume has to
# come from somewhere else when the plan text does not carry a size.
_CARDINALITY = re.compile(r"sizeInBytes|rowCount|Statistics\(")


@dataclass
class PlanShape:
    """The comparable part of a plan tree."""
    shuffles: int = 0
    broadcasts: int = 0
    reused_exchanges: int = 0
    aqe_shuffle_reads: int = 0
    skew_reads: int = 0
    nodes: int = 0


def shape_of(plan: dict[str, Any]) -> PlanShape:
    """Count the nodes of one ``sparkPlanInfo`` tree that mean the same in both plans.

    ``nodes`` is the total, and it is NOT comparable between a submitted and an
    executed plan: see the module docstring. It is kept because a zero would mean the
    tree failed to parse, which is worth seeing.
    """
    shape = PlanShape()

    def walk(node: dict[str, Any]) -> None:
        name = node.get("nodeName") or ""
        shape.nodes += 1
        if name == "Exchange":
            shape.shuffles += 1
        elif name == "BroadcastExchange":
            shape.broadcasts += 1
        elif name == "ReusedExchange":
            shape.reused_exchanges += 1
        elif name == "AQEShuffleRead":
            shape.aqe_shuffle_reads += 1
            if "skew" in (node.get("simpleString") or "").lower():
                shape.skew_reads += 1
        for child in node.get("children") or ():
            walk(child)

    walk(plan)
    return shape


@dataclass
class Execution:
    """One SQL execution's submitted plan, its last executed plan, and its names."""
    exec_id: int
    submitted: Optional[PlanShape] = None
    executed: Optional[PlanShape] = None
    replans: int = 0
    stage_ids: set[int] = field(default_factory=set)
    plan_sections: list[str] = field(default_factory=list)
    cardinality_hits: int = 0
    query_id: Optional[str] = None

    @property
    def drifted(self) -> bool:
        """Did the executed shuffle count differ from the submitted one?"""
        if self.submitted is None or self.executed is None:
            return False
        return self.executed.shuffles != self.submitted.shuffles


@dataclass
class Run:
    """One event log, with whatever the campaign recorded about the cell it ran in."""
    label: str
    executions: list[Execution]
    max_bw_gbs: Optional[float] = None
    latency_ms: Optional[float] = None

    def named(self) -> list[Execution]:
        """Executions the side-car named. An unnamed one cannot join across cells."""
        return [e for e in self.executions if e.query_id]


def _iter_events(zip_path: Path) -> Iterator[dict]:
    """Every JSON event of a Spark event log zip, in order. Bad lines are skipped."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if not names:
            return
        with zf.open(names[0]) as handle:
            for raw in handle:
                try:
                    event = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if isinstance(event, dict):
                    yield event


def read_log(zip_path: Path, sidecar: Optional[Sidecar] = None) -> list[Execution]:
    """One pass over an event log, returning one record per SQL execution.

    The submitted plan is the FIRST ``SQLExecutionStart`` for an execution id and the
    executed plan is the LAST ``SQLAdaptiveExecutionUpdate`` for it, because AQE
    re-reports the whole plan on every rewrite. An execution with no rewrite keeps its
    submitted plan as the executed one: nothing changed, and dropping it would bias the
    divergence rate upwards by removing the queries that agree.

    Stage ids come from ``JobStart``, whose properties carry ``spark.sql.execution.id``.
    They exist only to let the side-car name the execution.
    """
    executions: dict[int, Execution] = {}

    def slot(exec_id: Any) -> Optional[Execution]:
        try:
            key = int(exec_id)
        except (ValueError, TypeError):
            return None
        return executions.setdefault(key, Execution(exec_id=key))

    for event in _iter_events(zip_path):
        name = event.get("Event")
        if name == _SQL_START:
            record = slot(event.get("executionId"))
            if record is None:
                continue
            plan = event.get("sparkPlanInfo")
            if plan and record.submitted is None:
                record.submitted = shape_of(plan)
                text = event.get("physicalPlanDescription") or ""
                record.plan_sections = _PLAN_SECTION.findall(text)
                record.cardinality_hits = len(_CARDINALITY.findall(text))
        elif name == _SQL_ADAPTIVE:
            record = slot(event.get("executionId"))
            if record is None:
                continue
            plan = event.get("sparkPlanInfo")
            if plan:
                record.executed = shape_of(plan)
                record.replans += 1
        elif name == _JOB_START:
            props = event.get("Properties") or {}
            record = slot(props.get("spark.sql.execution.id"))
            if record is None:
                continue
            for stage_id in event.get("Stage IDs") or ():
                try:
                    record.stage_ids.add(int(stage_id))
                except (ValueError, TypeError):
                    continue

    for record in executions.values():
        if record.executed is None:
            record.executed = record.submitted
        if sidecar:
            record.query_id = sidecar.resolve(record.exec_id, frozenset(record.stage_ids))

    return [executions[k] for k in sorted(executions)]


def _load_sidecar(path: Optional[Path]) -> Optional[Sidecar]:
    if path is None or not path.is_file():
        return None
    try:
        return Sidecar.from_dict(json.loads(path.read_text()))
    except (json.JSONDecodeError, OSError, AttributeError):
        print(f"  WARNING: could not read the side-car {path}", file=sys.stderr)
        return None


def _cell_constants(cell: Path) -> tuple[Optional[float], Optional[float]]:
    """A cell's bandwidth cap and network latency, as the campaign stamped them.

    Nothing is inferred: a cell without a ``constants.json`` reports nulls and sorts
    last, rather than being given a made-up position in the grid.
    """
    path = cell / "constants.json"
    if not path.is_file():
        return None, None
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None, None
    def _num(key: str) -> Optional[float]:
        value = raw.get(key)
        return float(value) if isinstance(value, (int, float)) else None
    return _num("tsg_max_bw_gbs"), _num("network_latency_ms")


def load_campaign(results_dir: Path) -> list[Run]:
    """Every cell of a campaign results directory that has an event log.

    Cells are ordered by bandwidth descending then latency ascending. That is a display
    order computed from the stamped constants, not a claim that bandwidth dominates.
    """
    runs: list[Run] = []
    for cell in sorted(p for p in results_dir.iterdir() if p.is_dir()):
        logs = sorted((cell / "raw" / "spark").glob("*.log.zip"))
        if not logs:
            continue
        if len(logs) > 1:
            print(f"  WARNING: {cell.name} holds {len(logs)} event logs, reading all",
                  file=sys.stderr)
        sidecar = _load_sidecar(cell / "sidecar.json")
        executions: list[Execution] = []
        for log in logs:
            executions.extend(read_log(log, sidecar))
        bw, latency = _cell_constants(cell)
        runs.append(Run(label=cell.name, executions=executions,
                        max_bw_gbs=bw, latency_ms=latency))
        print(f"  read {cell.name}: {len(executions)} SQL executions", file=sys.stderr)

    def order(run: Run) -> tuple[float, float]:
        return (-(run.max_bw_gbs if run.max_bw_gbs is not None else -1.0),
                run.latency_ms if run.latency_ms is not None else float("inf"))

    return sorted(runs, key=order)


# --- 1. What the logs carry -------------------------------------------------


def section_logs_carry(runs: list[Run]) -> None:
    print("\n=== 1. What the event logs carry ===")
    print("  A submission-time predictor can only use what SQLExecutionStart holds, so")
    print("  this is the gate: no submitted plan means no feature extraction at all.")
    print(f"\n  {'cell':<20}{'bw Gb/s':>9}{'RTT ms':>8}{'execs':>7}{'planned':>9}"
          f"{'re-plans':>10}{'sections':>10}{'cardinality':>13}")
    for run in runs:
        planned = sum(1 for e in run.executions if e.submitted is not None)
        replans = sum(e.replans for e in run.executions)
        sections = {len(e.plan_sections) for e in run.executions if e.submitted is not None}
        cardinality = sum(e.cardinality_hits for e in run.executions)
        bw = f"{run.max_bw_gbs:.0f}" if run.max_bw_gbs is not None else "-"
        latency = f"{run.latency_ms:.3f}" if run.latency_ms is not None else "-"
        section_col = "/".join(str(s) for s in sorted(sections)) if sections else "-"
        print(f"  {run.label:<20}{bw:>9}{latency:>8}{len(run.executions):>7}{planned:>9}"
              f"{replans:>10}{section_col:>10}{cardinality:>13}")

    seen: dict[str, int] = defaultdict(int)
    for run in runs:
        for execution in run.executions:
            for name in execution.plan_sections:
                seen[name] += 1
    if seen:
        print("\n  Plan text sections found, with how many submitted plans carry each:")
        for name, count in sorted(seen.items(), key=lambda kv: -kv[1]):
            print(f"    {name:<32}{count:>7}")
    total_cardinality = sum(e.cardinality_hits for r in runs for e in r.executions)
    if total_cardinality == 0:
        print("\n  No sizeInBytes, rowCount or Statistics(...) anywhere in the plan text.")
        print("  Processed volume cannot be read from it. It has to come from the plan")
        print("  node metadata or from a catalog query, and which one is not settled here.")


# --- 2. Submitted against executed ------------------------------------------


def section_drift(runs: list[Run]) -> None:
    print("\n=== 2. Submitted shuffle count against executed ===")
    print("  Exchange nodes only. Broadcast, reused and wrapper nodes are excluded, and")
    print("  the module docstring says why each one is.")
    print(f"\n  {'cell':<20}{'named':>7}{'drifted':>9}{'share':>8}{'down':>6}{'up':>5}"
          f"{'shuffles lost':>15}{'skew reads':>12}")
    for run in runs:
        named = run.named()
        drifted = [e for e in named if e.drifted]
        down = sum(1 for e in drifted if e.executed.shuffles < e.submitted.shuffles)
        lost = sum(e.submitted.shuffles - e.executed.shuffles for e in drifted)
        skew = sum(e.executed.skew_reads for e in named if e.executed)
        share = f"{100 * len(drifted) / len(named):.0f}%" if named else "-"
        print(f"  {run.label:<20}{len(named):>7}{len(drifted):>9}{share:>8}"
              f"{down:>6}{len(drifted) - down:>5}{lost:>15}{skew:>12}")
    print("\n  'down' counts queries that executed FEWER shuffles than submitted. A")
    print("  predictor that trusts the submitted plan over-counts work for those.")


# --- 3. Does the cell decide? -----------------------------------------------


def _by_query(runs: list[Run]) -> dict[str, dict[str, Execution]]:
    out: dict[str, dict[str, Execution]] = defaultdict(dict)
    for run in runs:
        for execution in run.named():
            out[execution.query_id][run.label] = execution
    return out


def section_cell_dependence(runs: list[Run]) -> None:
    print("\n=== 3. Which queries drift, and whether the cell decides ===")
    per_query = _by_query(runs)
    complete = {q: d for q, d in per_query.items() if len(d) == len(runs)}
    print(f"  Queries present in all {len(runs)} cells: {len(complete)} of {len(per_query)}")
    if not complete:
        return

    varying_submitted = [q for q, d in complete.items()
                         if len({e.submitted.shuffles for e in d.values()}) > 1]
    print(f"  Submitted plan identical in every cell: "
          f"{len(complete) - len(varying_submitted)} of {len(complete)}")
    if varying_submitted:
        print(f"    but NOT for {', '.join(sorted(varying_submitted))}. The submitted plan")
        print("    is what a predictor reads, so a cell-dependent one breaks the premise.")

    always = sorted(q for q, d in complete.items() if all(e.drifted for e in d.values()))
    sometimes = sorted(q for q, d in complete.items()
                       if len({e.executed.shuffles for e in d.values()}) > 1)

    if always:
        print(f"\n  Drifts in EVERY cell, so the network is not the cause ({len(always)}):")
        print(f"    {'query':<12}{'submitted':>11}{'executed':>10}")
        for query in always:
            any_cell = next(iter(complete[query].values()))
            print(f"    {query:<12}{any_cell.submitted.shuffles:>11}"
                  f"{any_cell.executed.shuffles:>10}")

    if sometimes:
        print(f"\n  Executed count VARIES by cell ({len(sometimes)}). Cells left to right"
              " are")
        print("  bandwidth descending then latency ascending:")
        header = "".join(f"{r.label.replace('onprem-baseline', 'onprem')[:10]:>11}"
                         for r in runs)
        print(f"    {'query':<10}{'submitted':>10}{header}")
        for query in sometimes:
            row = "".join(f"{complete[query][r.label].executed.shuffles:>11}" for r in runs)
            submitted = next(iter(complete[query].values())).submitted.shuffles
            print(f"    {query:<10}{submitted:>10}{row}")
        print("\n  This is NOT reported as a transport effect. AQE decides on statistics")
        print("  that arrive when a shuffle materialises, so the decision is timing-")
        print("  dependent, and one run per cell leaves nondeterminism and the regime")
        print("  confounded. Repeat these queries at the cells on either side of each")
        print("  change to separate them.")
    else:
        print("\n  No query's executed shuffle count depends on the cell.")


# --- 4. What the broadcast delta is not -------------------------------------


def section_broadcast(runs: list[Run]) -> None:
    print("\n=== 4. The broadcast delta, and what it does not mean ===")
    print(f"  {'cell':<20}{'broadcast grew':>16}{'reuse grew':>12}{'coalesced':>11}")
    for run in runs:
        named = run.named()
        grew = sum(1 for e in named
                   if e.submitted and e.executed
                   and e.executed.broadcasts > e.submitted.broadcasts)
        reuse = sum(1 for e in named
                    if e.submitted and e.executed
                    and e.executed.reused_exchanges > e.submitted.reused_exchanges)
        coalesced = sum(1 for e in named if e.executed and e.executed.aqe_shuffle_reads)
        print(f"  {run.label:<20}{grew:>16}{reuse:>12}{coalesced:>11}")
    print("\n  'broadcast grew' is mostly dynamic partition pruning materialising, not a")
    print("  join strategy change: SubqueryAdaptiveBroadcast becomes SubqueryBroadcast")
    print("  plus a real BroadcastExchange. Read section 2 for strategy changes.")


def to_records(runs: list[Run]) -> list[dict]:
    """One row per cell and query, for whatever wants the numbers rather than the report."""
    rows = []
    for run in runs:
        for execution in run.executions:
            rows.append({
                "cell": run.label,
                "tsg_max_bw_gbs": run.max_bw_gbs,
                "network_latency_ms": run.latency_ms,
                "query_id": execution.query_id,
                "sql_execution_id": execution.exec_id,
                "replans": execution.replans,
                "plan_sections": execution.plan_sections,
                "cardinality_hits": execution.cardinality_hits,
                "submitted_shuffles":
                    execution.submitted.shuffles if execution.submitted else None,
                "executed_shuffles":
                    execution.executed.shuffles if execution.executed else None,
                "submitted_broadcasts":
                    execution.submitted.broadcasts if execution.submitted else None,
                "executed_broadcasts":
                    execution.executed.broadcasts if execution.executed else None,
                "aqe_shuffle_reads":
                    execution.executed.aqe_shuffle_reads if execution.executed else None,
                "skew_reads": execution.executed.skew_reads if execution.executed else None,
            })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="How far does the executed plan drift from the submitted one?")
    parser.add_argument("--results", type=Path,
                        help="a campaign results directory holding <cell>/raw/spark/*.log.zip")
    parser.add_argument("--log", type=Path, help="a single event log zip")
    parser.add_argument("--sidecar", type=Path, help="query names for --log")
    parser.add_argument("--label", default="run", help="the cell name for --log")
    parser.add_argument("--output", type=Path, help="write the per-query rows as JSON")
    args = parser.parse_args()

    if bool(args.results) == bool(args.log):
        sys.exit("Error: pass exactly one of --results and --log.")

    if args.results:
        if not args.results.is_dir():
            sys.exit(f"Error: {args.results} is not a directory.")
        runs = load_campaign(args.results)
        if not runs:
            sys.exit(f"Error: no cell under {args.results} has raw/spark/*.log.zip.")
    else:
        if not args.log.is_file():
            sys.exit(f"Error: {args.log} does not exist.")
        runs = [Run(label=args.label,
                    executions=read_log(args.log, _load_sidecar(args.sidecar)))]

    unnamed = sum(len(r.executions) - len(r.named()) for r in runs)
    if unnamed:
        print(f"  {unnamed} SQL executions are unnamed and are left out of sections 2 to 4."
              " Pass the side-car to name them.", file=sys.stderr)

    section_logs_carry(runs)
    section_drift(runs)
    if len(runs) > 1:
        section_cell_dependence(runs)
    section_broadcast(runs)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(to_records(runs), indent=1))
        print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()
