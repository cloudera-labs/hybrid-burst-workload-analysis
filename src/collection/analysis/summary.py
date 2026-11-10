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

"""Summarize a processed Impala or Spark workload.

The command reads the processed data under ``data/processed/<engine>``. It prints
the data volume, the network rate, the risk mix, and the compute you can move to
the cloud. For Impala it also prints the peak memory and the smallest node count.
For Spark it prints the executor use.

The command reads only the processed data. It does not call the cluster. Run the
fetch and process commands first.

Usage:
    python -m src.collection.analysis.summary --engine spark
    python -m src.collection.analysis.summary --engine impala
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path

from .ranking import (
    RankedJob,
    _fmt_bytes,
    _fmt_intensity,
    _fmt_span,
    _load_source,
    _observed_range,
    _parse_impala_queries,
    _score_jobs,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _span_seconds(from_iso: str, to_iso: str) -> float:
    """Return the seconds between two ISO-8601 timestamps, or 0 on a parse error."""
    try:
        a = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
        b = datetime.fromisoformat(to_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return 0.0
    return max(0.0, (b - a).total_seconds())


def _load_raw_impala(source: str) -> list[dict]:
    """Load the processed Impala records as raw dicts, deduplicated by queryId."""
    p = Path(source)
    hint = "  Run: make download ENGINE=impala && make process ENGINE=impala"
    if p.is_dir():
        files = sorted(p.glob("impala_*.json"))
        if not files:
            sys.exit(f"Error: no processed impala data in {source}.\n{hint}")
    elif p.exists():
        files = [p]
    else:
        sys.exit(f"Error: no processed impala data at {source}.\n{hint}")

    raw: list[dict] = []
    seen: set = set()
    for f in files:
        with open(f) as fh:
            for q in json.load(fh):
                qid = q.get("queryId")
                if qid is not None and qid in seen:
                    continue
                if qid is not None:
                    seen.add(qid)
                raw.append(q)
    return raw


def _parse_memory_records(queries: list[dict]) -> list[dict]:
    """Return [{start_ts, end_ts, total_memory, mem_per_node}] for finished queries.

    total_memory is memory_per_node_peak × num_backends — the memory the query held
    across all its nodes at the same time.
    """
    records: list[dict] = []
    for q in queries:
        if q.get("queryState") != "FINISHED":
            continue
        duration_ms = q.get("durationMillis", 0)
        if duration_ms <= 0:
            continue
        attrs = q.get("attributes", {})
        mem_per_node = attrs.get("memory_per_node_peak")
        num_backends = attrs.get("num_backends")
        start_str = q.get("startTime", "")
        if not mem_per_node or not num_backends or not start_str:
            continue
        try:
            mem = float(mem_per_node)
            backends = int(float(num_backends))
            start_ts = datetime.fromisoformat(start_str.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            continue
        records.append({
            "start_ts": start_ts,
            "end_ts": start_ts + duration_ms / 1000.0,
            "total_memory": mem * backends,
            "mem_per_node": mem,
        })
    return records


def _peak_concurrent_memory(records: list[dict]) -> float:
    """Return the peak total memory in use at any instant.

    The function sweeps the start and end events in time order. It adds memory at a
    start and removes it at an end. The high-water mark is the peak concurrent demand.
    """
    events: list[tuple[float, float]] = []
    for r in records:
        events.append((r["start_ts"], r["total_memory"]))
        events.append((r["end_ts"], -r["total_memory"]))
    events.sort(key=lambda x: (x[0], -x[1]))

    current = peak = 0.0
    for _, delta in events:
        current += delta
        if current > peak:
            peak = current
    return peak


# ─── Sections ────────────────────────────────────────────────────────────────


def _common_lines(jobs: list[RankedJob], engine: str) -> list[str]:
    """Build the sections that apply to both engines."""
    range_from, range_to = _observed_range(jobs)
    total_read = sum(j.bytes_read for j in jobs)
    total_written = sum(j.bytes_written for j in jobs)
    total_bytes = total_read + total_written

    lines = [f"── Workload summary ({engine}) ──"]
    if range_from and range_to:
        span = _fmt_span(range_from, range_to)
        span_str = f"  ({span})" if span else ""
        lines.append(f"  Data range:      {range_from} → {range_to}{span_str}")
    lines.append(f"  Executions:      {len(jobs)}")
    lines.append("")

    lines.append("── Data volume ──")
    lines.append(f"  Total read:      {_fmt_bytes(total_read)}")
    lines.append(f"  Total written:   {_fmt_bytes(total_written)}")
    span_s = _span_seconds(range_from, range_to)
    if span_s > 0:
        lines.append(f"  Average rate:    {_fmt_intensity(total_bytes / span_s)}"
                     "  (read + written over the data range)")
    peak_rate = max(
        ((j.bytes_read + j.bytes_written) / j.exec_active_s for j in jobs if j.exec_active_s > 0),
        default=0.0,
    )
    lines.append(f"  Peak rate:       {_fmt_intensity(peak_rate)}  (busiest single workload)")
    lines.append("")

    buckets: dict[str, list[RankedJob]] = {"Low": [], "Medium": [], "High": []}
    for j in jobs:
        buckets[j.risk].append(j)

    lines.append("── Disaggregation risk ──")
    for risk in ("Low", "Medium", "High"):
        b = buckets[risk]
        pct = len(b) * 100 / len(jobs) if jobs else 0
        r = sum(j.bytes_read for j in b)
        w = sum(j.bytes_written for j in b)
        lines.append(f"  {risk + ':':<8}{len(b):>6} ({pct:4.1f}%)   "
                     f"read={_fmt_bytes(r)}  written={_fmt_bytes(w)}")
    lines.append("  A Low-risk workload is safe to move. A High-risk workload reads a lot of")
    lines.append("  data over the network, so it stays on premises.")
    lines.append("")

    total_cpu_ms = sum(j.cpu_time_ms for j in jobs)
    if total_cpu_ms > 0:
        total_cpu_s = total_cpu_ms / 1000
        low_cpu_s = sum(j.cpu_time_ms for j in buckets["Low"]) / 1000
        low_pct = low_cpu_s * 100 / total_cpu_s if total_cpu_s else 0
        lines.append("── Compute you can offload ──")
        lines.append(f"  Total CPU-time:  {total_cpu_s:,.0f} CPU-seconds  ({total_cpu_s / 3600:.1f} CPU-hours)")
        lines.append(f"  Low-risk share:  {low_pct:.1f}%  (move this share of the compute to the cloud)")
        lines.append("")
    return lines


def _impala_memory_lines(raw: list[dict]) -> list[str]:
    """Build the Impala memory and node-count section."""
    records = _parse_memory_records(raw)
    if not records:
        return []
    peak = _peak_concurrent_memory(records)
    pool = max((r["mem_per_node"] for r in records), default=0.0) * 1.1

    lines = ["── Memory and node count ──"]
    lines.append(f"  Peak concurrent memory:  {peak / 1e9:.1f} GB")
    if pool > 0:
        lines.append(f"  Memory per node:         {pool / 1e9:.1f} GB  (the largest query seen, plus 10%)")
        for headroom in (0.0, 0.3):
            effective = pool * (1 - headroom)
            nodes = math.ceil(peak / effective) if effective > 0 else 0
            lines.append(f"  Smallest node count:     {nodes} nodes at {int(headroom * 100)}% headroom")
    lines.append("  Headroom is spare memory. It protects against demand above the window you saw.")
    return lines


def _time_breakdown_lines(jobs: list[RankedJob], engine: str) -> list[str]:
    """Build the wall-clock time breakdown (resource wait / planning / active).

    Wall-clock splits three ways and the parts sum exactly: resource wait + planning/startup
    + active execution. The parts are summed over every execution, so the total is aggregate
    app-elapsed (runs overlap in real time) — read the percentages, not the absolute hours.

    The three slices are the same idea for both engines, so the terminology matches:
      * Spark  -- resource wait is the pre-first-executor wait (clamped to overhead); active
                  is the stage span; planning/startup is admission/planning/driver/fetch. A
                  fourth figure, executor-slot contention, sits INSIDE active (it double
                  counts against active), so it is printed only as "would also shrink".
      * Impala -- resource wait is the admission-queue wait (query queued, nothing runs);
                  active is the query executing; planning/startup is compile plus the client
                  result fetch. Impala has no contention figure (contention_tax_s is 0), so
                  that trailing line does not appear.

    Both slices that are capacity-recoverable (resource wait, and Spark contention) shrink
    with more capacity. The rest (planning, client fetch) stays.
    """
    wall = sum(j.duration_s for j in jobs)
    if wall <= 0:
        return []
    is_spark = engine == "spark"
    active = sum(j.exec_active_s for j in jobs)
    resource_wait = sum(j.resource_wait_s for j in jobs)
    planning = max(0.0, wall - active - resource_wait)
    contention = sum(j.contention_tax_s for j in jobs)

    # Engine-specific wording for the same three phases.
    wait_note = "no executor yet, nothing runs" if is_spark else "queued for admission, nothing runs"
    plan_note = "admission, planning, driver, fetch" if is_spark else "compile/plan, then client result fetch"
    active_note = "stages actually running" if is_spark else "the query actually executing"
    cap_word = "executors" if is_spark else "admission capacity"

    def pct(x: float) -> float:
        return x * 100 / wall

    # Every phase is also split by disaggregation risk (indented), all as a share of
    # wall-clock, so each risk trio sums to its parent line. The WAN-bound (High) jobs tend
    # to dominate every phase even when they are fewer.
    def by_risk(value) -> list[str]:
        out = []
        for risk in ("Low", "Medium", "High"):
            v = sum(value(j) for j in jobs if j.risk == risk)
            out.append(f"    {risk + ' risk:':<18}{v / 3600:8.1f} h  ({pct(v):4.1f}%)")
        return out

    lines = ["── Time breakdown ──"]
    lines.append(f"  Wall-clock:         {wall / 3600:8.1f} h  (summed over all executions)")
    lines += by_risk(lambda j: j.duration_s)
    lines.append(f"  Resource wait:      {resource_wait / 3600:8.1f} h  ({pct(resource_wait):4.1f}%)  {wait_note}")
    lines += by_risk(lambda j: j.resource_wait_s)
    lines.append(f"  Planning/startup:   {planning / 3600:8.1f} h  ({pct(planning):4.1f}%)  {plan_note}")
    lines += by_risk(lambda j: max(0.0, j.duration_s - j.exec_active_s - j.resource_wait_s))
    lines.append(f"  Active processing:  {active / 3600:8.1f} h  ({pct(active):4.1f}%)  {active_note}")
    lines += by_risk(lambda j: j.exec_active_s)
    lines.append(f"  With enough {cap_word} the resource wait approaches zero, leaving "
                 f"{pct(planning) + pct(active):.1f}% of wall-clock.")
    if contention > 0:
        lines.append(f"  Executor-slot contention inside active: {contention / 3600:.1f} h "
                     f"({pct(contention):.1f}% of wall-clock) would also shrink.")
    return lines


def _bursting_lines(jobs: list[RankedJob], engine: str) -> list[str]:
    """The on-prem wall-clock bursting could reclaim, in the ideal case.

    The reclaimable total sums three contributors, indented below it:
      * Move Low    -- the whole Low workload runs in the cloud, so its full wall-clock
                       leaves on-prem (it is WAN-safe: little data, tiny WAN penalty).
      * Move Medium -- likewise for Medium (WAN-safe after a test).
      * Relieve High -- the High workload STAYS on-prem (too data-heavy for the WAN), but
                       freeing the Low + Medium slots lets it wait less. For Spark that is
                       its resource wait (a first executor sooner) plus its contention (fewer
                       mid-run slot waits); for Impala it is its admission wait (admitted
                       sooner) only, since Impala has no contention figure. The active
                       compute is irreducible and is NOT reclaimed.

    Moving a workload takes its whole wall-clock, which already carries that workload's own
    resource wait (and, for Spark, contention), so those are booked once under Move and never
    double counted under Relieve. The percentages are shares of total wall-clock, so the
    three contributors sum to the reclaimable share.

    This is an UPPER BOUND / ideal case: it assumes warm cloud capacity (the moved jobs pay
    no resource wait in the cloud) and that freeing the on-prem slots fully relieves the High
    wait. In reality the High jobs still compete among themselves and cloud capacity has its
    own acquisition latency, so the achieved figure is lower.
    """
    wall = sum(j.duration_s for j in jobs)
    if wall <= 0:
        return []
    is_spark = engine == "spark"
    buckets: dict[str, list[RankedJob]] = {"Low": [], "Medium": [], "High": []}
    for j in jobs:
        buckets[j.risk].append(j)

    low_wall = sum(j.duration_s for j in buckets["Low"])
    med_wall = sum(j.duration_s for j in buckets["Medium"])
    high_rw = sum(j.resource_wait_s for j in buckets["High"])
    high_con = sum(j.contention_tax_s for j in buckets["High"]) if is_spark else 0.0
    high_relief = high_rw + high_con
    total = low_wall + med_wall + high_relief

    def pct(x: float) -> float:
        return x * 100 / wall

    def row(label: str, secs: float, note: str) -> str:
        return f"  {label:<27}{secs / 3600:6.1f} h  {note}"

    def subrow(label: str, secs: float, note: str) -> str:
        return f"    {label:<25}{secs / 3600:6.1f} h  ({pct(secs):4.1f}%)  {note}"

    # Spark names both recoverable pools; Impala has admission wait only.
    if is_spark:
        relief_note = (f"High wait the freed slots may lower ({high_rw / 3600:.1f} h resource "
                       f"wait + {high_con / 3600:.1f} h contention)")
    else:
        relief_note = "High admission wait the freed capacity may lower"

    lines = ["── Bursting potential ──"]
    lines.append("  The on-prem wall-clock bursting could reclaim in the ideal case. It moves the")
    lines.append("  WAN-safe Low and Medium workloads to the cloud, and frees their slots so the")
    lines.append("  High workload that stays waits less. It is an upper bound (see below).")
    lines.append(row("Reclaimable:", total, f"({pct(total):.1f}% of wall-clock, ideal case)"))
    lines.append(subrow("Move Low:", low_wall, "Low wall-clock, offloaded to the cloud"))
    lines.append(subrow("Move Medium:", med_wall, "Medium wall-clock, offloaded (test first)"))
    lines.append(subrow("Relieve High:", high_relief, relief_note))
    lines.append("  Ideal case: it assumes warm cloud capacity and that freed slots fully relieve")
    lines.append("  the High jobs, which still compete among themselves, so the real figure is lower.")
    return lines


def _spark_executor_lines(jobs: list[RankedJob]) -> list[str]:
    """Build the Spark executor-use section."""
    peak = max((j.exec_peak for j in jobs), default=0)
    executor_seconds = sum(j.exec_seconds for j in jobs)
    core_seconds = sum(j.exec_seconds * j.exec_cores for j in jobs)
    if peak == 0 and executor_seconds == 0:
        return []

    lines = ["── Executor use ──"]
    lines.append(f"  Peak executors:  {peak}  (the most one application used at the same time)")
    if executor_seconds > 0:
        lines.append(f"  Executor-time:   {executor_seconds / 3600:.1f} executor-hours")
    if core_seconds > 0:
        lines.append(f"  Core-time:       {core_seconds / 3600:.1f} core-hours")
    return lines


# ─── Main ────────────────────────────────────────────────────────────────────


def summarize(engine: str, source: str) -> str:
    """Load the processed data for the engine and return the summary text."""
    if engine == "impala":
        raw = _load_raw_impala(source)
        jobs = _score_jobs(_parse_impala_queries(raw))
        if not jobs:
            sys.exit("Error: no finished queries found in the processed data.")
        lines = (_common_lines(jobs, engine) + _time_breakdown_lines(jobs, engine)
                 + _bursting_lines(jobs, engine) + _impala_memory_lines(raw))
    else:
        jobs = _score_jobs(_load_source(source, "spark"))
        if not jobs:
            sys.exit("Error: no applications found in the processed data.")
        lines = (_common_lines(jobs, engine) + _time_breakdown_lines(jobs, engine)
                 + _bursting_lines(jobs, engine) + _spark_executor_lines(jobs))
    return "\n".join(lines).rstrip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize a processed Impala or Spark workload")
    parser.add_argument("--engine", choices=["impala", "spark"], default="spark",
                        help="Engine to summarize (default: spark)")
    args = parser.parse_args()

    print(summarize(args.engine, f"data/processed/{args.engine}"))


if __name__ == "__main__":
    main()
