"""Rank workloads by disaggregation sensitivity (bandwidth + latency dimensions).

Produces a scored ranking from least impacted (safe to offload) to most impacted.
It reads the processed data under ``data/processed/<engine>`` — the output of the
download and process steps. Run those first.

Usage:
    python -m src.collection.analysis.ranking --engine impala
    python -m src.collection.analysis.ranking --engine spark --json
    python -m src.collection.analysis.ranking --engine impala --no-group --save-json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


REF_DATA_BYTES = 3e9
REF_INTENSITY_BPS = 1e9 / 8
REF_FILE_COUNT = 1000


@dataclass
class RankedJob:
    name: str
    duration_s: float          # wall-clock (includes admission/queue/idle)
    exec_active_s: float       # active execution time (stages actually running)
    bytes_read: float
    bytes_written: float
    file_count: int
    score: float
    data_score: float
    intensity_score: float
    file_score: float
    risk: str
    why: str
    runs: int = 1
    signature: str = ""
    cpu_time_ms: int = 0
    # Executor-acquisition wait summed across stages (Spark only; 0 for Impala and the
    # live History-Server path). Time tasks were pending for an executor slot — it lives
    # INSIDE exec_active_s, so it is separate from overhead (duration − active).
    contention_tax_s: float = 0.0
    # Pre-execution resource-acquisition wait (Spark only; 0 for Impala and the live path).
    # Wall-clock before the first executor existed, clamped to overhead. It is the slice of
    # overhead that would vanish with enough capacity, so overhead splits cleanly into
    # resource_wait_s (waiting for a slot) + the remainder (planning/startup/tail).
    resource_wait_s: float = 0.0
    # Engine-reported statement kind: Spark "sql"/"rdd", Impala "query"/"ddl"/"dml"/…
    # ("" when the source path doesn't expose it, e.g. live History Server fetch).
    job_type: str = ""
    # Spark resource footprint (0 for engines/paths that don't expose it). exec_seconds is
    # the ∫ concurrent-executors dt integral; exec_avg = exec_seconds / duration (mean
    # concurrent executors, churn-proof); exec_peak is the high-water executor count.
    exec_peak: int = 0
    exec_avg: float = 0.0
    exec_seconds: float = 0.0
    exec_cores: int = 0
    exec_mem_gb: float = 0.0
    # spark.dynamicAllocation.enabled: True (dynamic), False (static), None (unknown).
    dynamic_allocation: bool | None = None
    # Observed wall-clock window of the underlying run(s), ISO-8601 (empty if unknown).
    # When grouped, start_time is the earliest run and end_time the latest.
    start_time: str = ""
    end_time: str = ""
    # Per-run standard deviation across the group's runs (0 for single-run groups).
    # When grouped, duration_s/exec_active_s/bytes_read/bytes_written hold the per-run
    # MEAN, and these hold its spread, so the ranking shows mean ± std.
    duration_std_s: float = 0.0
    exec_active_std_s: float = 0.0
    overhead_std_s: float = 0.0        # std of per-run (duration − active)
    contention_tax_std_s: float = 0.0  # std of per-run contention_tax_s
    resource_wait_std_s: float = 0.0   # std of per-run resource_wait_s
    planning_startup_std_s: float = 0.0  # std of per-run max(0, duration − active − resource_wait)
    bytes_read_std: float = 0.0
    bytes_written_std: float = 0.0
    cpu_time_ms_std: float = 0.0


# ─── Impala parsing ──────────────────────────────────────────────────────────


def _parse_impala_queries(queries: list[dict]) -> list[RankedJob]:
    """Convert raw CM API query dicts into RankedJob objects."""
    jobs: list[RankedJob] = []
    for q in queries:
        if q.get("queryState") != "FINISHED":
            continue
        duration_ms = q.get("durationMillis", 0)
        if duration_ms <= 0:
            continue

        attrs = q.get("attributes", {})
        hdfs_bytes_read = float(attrs.get("hdfs_bytes_read", 0))
        hdfs_bytes_written = float(attrs.get("hdfs_bytes_written", 0))
        avg_scan_range = float(attrs.get("hdfs_average_scan_range", 0))

        duration_s = duration_ms / 1000.0
        file_count = int(hdfs_bytes_read / avg_scan_range) if avg_scan_range > 0 else 0

        # Impala wall-clock splits the same three ways as Spark, so the summary and the
        # Overhead column mean the same thing across engines. All three CM counters are in
        # milliseconds, like durationMillis:
        #   * admission_wait          -> resource_wait_s: the query sat in the admission
        #     queue with no resources granted, so nothing ran. This is the Impala analog of
        #     Spark's "waiting for the first executor" wait, and it is the slice that shrinks
        #     with more admission capacity.
        #   * planning_wait_time      -> part of overhead (compile/plan the statement).
        #   * client_fetch_wait_time  -> part of overhead (the coordinator holds the query
        #     open while the client pages results; it is idle, not computing). This is
        #     usually the biggest non-active slice, but it is client-bound, NOT capacity
        #     bound, so more compute does not shrink it.
        # exec_active_s is the residual: the wall-clock the query was actually executing.
        # It drives the intensity score (bytes moved per active second), matching Spark,
        # where exec_active_s already excludes the queue/idle time. The residual is clamped
        # to >= 0 for the rare row whose counters overlap; missing counters default to 0, so
        # a row with no timing attributes falls back to exec_active_s == duration_s.
        admission_wait_s = float(attrs.get("admission_wait", 0) or 0) / 1000.0
        planning_wait_s = float(attrs.get("planning_wait_time", 0) or 0) / 1000.0
        client_fetch_wait_s = float(attrs.get("client_fetch_wait_time", 0) or 0) / 1000.0
        active_s = max(0.0, duration_s - admission_wait_s - planning_wait_s - client_fetch_wait_s)

        statement = q.get("statement", "") or q.get("queryId", "unknown")
        cpu_time_ms = int(attrs.get("thread_cpu_time", 0))

        jobs.append(RankedJob(
            name=statement,
            duration_s=duration_s,
            exec_active_s=active_s,
            bytes_read=hdfs_bytes_read,
            bytes_written=hdfs_bytes_written,
            file_count=file_count,
            score=0.0,
            data_score=0.0,
            intensity_score=0.0,
            file_score=0.0,
            risk="",
            why="",
            signature=statement,
            cpu_time_ms=cpu_time_ms,
            # Admission-queue wait = the capacity-recoverable "resource wait" phase. The
            # remaining overhead (planning + client fetch) is derived by the summary as
            # duration - active - resource_wait, so it needs no separate field.
            resource_wait_s=admission_wait_s,
            job_type=(q.get("queryType", "") or "").lower(),
            start_time=q.get("startTime", "") or "",
            end_time=q.get("endTime", "") or "",
        ))

    return jobs


def _load_source(source: str, engine: str) -> list[RankedJob]:
    """Load processed records from a file or a directory.

    Spark reads the parsed event-log output (``measurement_summary.json``). Impala
    reads the processed CM query exports (``impala_*.json``), concatenating every
    file in the directory and deduplicating by queryId.
    """
    p = Path(source)

    # Spark, current method: parsed event-log job summaries.
    if engine == "spark":
        meas = (p / "measurement_summary.json") if p.is_dir() else p
        if meas.name == "measurement_summary.json" and meas.exists():
            print(f"Loading from {meas}", file=sys.stderr)
            with open(meas) as f:
                return _parse_spark_measurements(json.load(f))

    if p.is_dir():
        files = sorted(p.glob(f"{engine}_*.json"))
        if not files:
            sys.exit(f"Error: no processed {engine} data in {source}.\n"
                     f"  Run: make download ENGINE={engine} && make process ENGINE={engine}")
        # Read the FULL downloaded range: concatenate every matching file so newly
        # downloaded windows are included, not just the latest. Dedup by queryId
        # (Impala) so overlapping re-downloads don't double-count — exactness matters
        # for the count distribution.
        queries = []
        seen: set = set()
        dropped = 0
        for f in files:
            with open(f) as fh:
                batch = json.load(fh)
            for q in batch:
                qid = q.get("queryId") if engine == "impala" else q.get("id")
                if qid is not None and qid in seen:
                    dropped += 1
                    continue
                if qid is not None:
                    seen.add(qid)
                queries.append(q)
        note = f", {dropped} duplicates dropped" if dropped else ""
        print(f"Loaded {len(files)} file(s) from {p}: {len(queries)} {engine} records{note}",
              file=sys.stderr)
        if engine == "impala":
            return _parse_impala_queries(queries)
        elif engine == "spark":
            return _parse_spark_apps(queries)
        else:
            sys.exit(f"Error: unknown engine '{engine}' for source loading")

    if not p.exists():
        sys.exit(f"Error: no processed {engine} data at {p}.\n"
                 f"  Run: make download ENGINE={engine} && make process ENGINE={engine}")
    print(f"Loading from {p}", file=sys.stderr)
    with open(p) as f:
        queries = json.load(f)
    if engine == "impala":
        return _parse_impala_queries(queries)
    elif engine == "spark":
        return _parse_spark_apps(queries)
    else:
        sys.exit(f"Error: unknown engine '{engine}' for source loading")


# ─── Spark parsing ─────────────────────────────────────────────────────────────


def _parse_spark_apps(apps: list[dict]) -> list[RankedJob]:
    """Convert saved Spark app dicts (with embedded stages) into RankedJob objects."""
    jobs: list[RankedJob] = []
    for app in apps:
        duration_s = app.get("_duration_s", 0)
        if duration_s <= 0:
            continue
        total_input = app.get("_bytes_read", 0)
        total_output = app.get("_bytes_written", 0)
        scan_tasks = app.get("_file_count", 0)
        name = app.get("name", app.get("id", "unknown"))

        jobs.append(RankedJob(
            name=name,
            duration_s=duration_s,
            exec_active_s=duration_s,   # no per-stage breakdown on this path; fall back to wall-clock
            bytes_read=float(total_input),
            bytes_written=float(total_output),
            file_count=scan_tasks,
            score=0.0,
            data_score=0.0,
            intensity_score=0.0,
            file_score=0.0,
            risk="",
            why="",
        ))
    return jobs


def _parse_spark_measurements(records: list[dict]) -> list[RankedJob]:
    """Convert parsed event-log job summaries (measurement_summary.json) into RankedJobs.

    This is the offline path produced by spark_log_pipeline from downloaded event logs.
    Storage I/O (input read + output write) drives the disaggregation score; shuffle is
    inter-executor traffic and intentionally excluded. duration_s is app wall-clock
    (exec_time_s) — it includes admission/queue idle, which mainly deflates the bandwidth
    dimension; the data and file dimensions are duration-independent. exec_active_s is the
    active stage span (active_exec_time_s from the parser), bounded by wall-clock and far
    below it when a job queues. Grouping key is match_id (already normalized by the
    parser), so the SQL normalizer in _group_jobs is a safe no-op on it.
    """
    GB = 1024 ** 3
    jobs: list[RankedJob] = []
    for r in records:
        duration_s = float(r.get("exec_time_s", 0) or 0)
        if duration_s <= 0:
            continue
        bytes_read = float(r.get("spark_total_input_read_gb", 0) or 0) * GB
        bytes_written = float(r.get("spark_total_output_write_gb", 0) or 0) * GB
        file_count = int(r.get("total_files_read") or 0)
        cpu_time_ms = int(float(r.get("spark_total_executor_cpu_time_s", 0) or 0) * 1000)
        active_s = float(r.get("active_exec_time_s", 0) or 0)
        query_id = r.get("query_id") or r.get("app_id", "")
        match_id = r.get("match_id") or query_id or "unknown"
        name = r.get("app_name") or match_id

        jobs.append(RankedJob(
            name=name,
            duration_s=duration_s,
            exec_active_s=active_s,
            bytes_read=bytes_read,
            bytes_written=bytes_written,
            file_count=file_count,
            score=0.0,
            data_score=0.0,
            intensity_score=0.0,
            file_score=0.0,
            risk="",
            why="",
            signature=match_id,
            cpu_time_ms=cpu_time_ms,
            contention_tax_s=float(r.get("contention_tax_s", 0) or 0),
            resource_wait_s=float(r.get("resource_wait_s", 0) or 0),
            job_type=r.get("type", "") or "",
            exec_peak=int(r.get("spark_num_executors", 0) or 0),
            exec_avg=float(r.get("spark_avg_executors", 0) or 0),
            exec_seconds=float(r.get("spark_executor_seconds", 0) or 0),
            exec_cores=int(r.get("spark_executor_cores", 0) or 0),
            exec_mem_gb=float(r.get("spark_executor_memory_gb", 0) or 0),
            dynamic_allocation=r.get("spark_dynamic_allocation", None),
            start_time=r.get("start_time", "") or "",
            end_time=r.get("end_time", "") or "",
        ))
    return jobs


# ─── Grouping ─────────────────────────────────────────────────────────────────


_RE_STRINGS = re.compile(r"'[^']*'")
_RE_NUMBERS = re.compile(r"\b\d+\.?\d*\b")
_RE_WHITESPACE = re.compile(r"\s+")


def _normalize_query(text: str) -> str:
    """Normalize SQL by replacing literals with placeholders."""
    text = _RE_STRINGS.sub("?", text)
    text = _RE_NUMBERS.sub("?", text)
    text = _RE_WHITESPACE.sub(" ", text).strip()
    return text


def _group_jobs(jobs: list[RankedJob]) -> tuple[list[RankedJob], dict]:
    """Group jobs by normalized full statement, aggregating metrics.

    Returns (grouped_jobs, quality_stats).
    """
    import numpy as np

    groups: dict[str, list[RankedJob]] = {}
    for job in jobs:
        key = _normalize_query(job.signature or job.name)
        groups.setdefault(key, []).append(job)

    grouped: list[RankedJob] = []
    for key, members in groups.items():
        reads = np.array([j.bytes_read for j in members], dtype=float)
        writes = np.array([j.bytes_written for j in members], dtype=float)
        durs = np.array([j.duration_s for j in members], dtype=float)
        actives = np.array([j.exec_active_s for j in members], dtype=float)
        files = np.array([j.file_count for j in members], dtype=float)
        cpus = np.array([j.cpu_time_ms for j in members], dtype=float)
        contentions = np.array([j.contention_tax_s for j in members], dtype=float)
        res_waits = np.array([j.resource_wait_s for j in members], dtype=float)

        # Per-run mean characterizes a single typical execution (the WAN-relevant unit);
        # std shows how consistent the runs are. Intensity from means equals intensity
        # from sums (the run-count cancels), so it is recurrence-frequency independent.
        mean_read = float(reads.mean())
        mean_written = float(writes.mean())
        mean_dur = float(durs.mean())

        starts = [j.start_time for j in members if j.start_time]
        ends = [j.end_time for j in members if j.end_time]
        types = {j.job_type for j in members if j.job_type}
        job_type = next(iter(types)) if len(types) == 1 else ("mixed" if types else "")
        exec_peaks = np.array([j.exec_peak for j in members], dtype=float)
        exec_avgs = np.array([j.exec_avg for j in members], dtype=float)
        exec_secs = np.array([j.exec_seconds for j in members], dtype=float)
        exec_cores_arr = np.array([j.exec_cores for j in members], dtype=float)
        exec_mems = np.array([j.exec_mem_gb for j in members], dtype=float)
        dyn_vals = {j.dynamic_allocation for j in members if j.dynamic_allocation is not None}
        dynamic_allocation = next(iter(dyn_vals)) if len(dyn_vals) == 1 else None

        grouped.append(RankedJob(
            name=key,
            duration_s=mean_dur,
            exec_active_s=float(actives.mean()),
            bytes_read=mean_read,
            bytes_written=mean_written,
            file_count=int(round(float(files.mean()))),
            score=0.0,
            data_score=0.0,
            intensity_score=0.0,
            file_score=0.0,
            risk="",
            why="",
            runs=len(members),
            cpu_time_ms=int(round(float(cpus.mean()))),
            contention_tax_s=float(contentions.mean()),
            resource_wait_s=float(res_waits.mean()),
            job_type=job_type,
            exec_peak=int(round(float(exec_peaks.mean()))),
            exec_avg=float(exec_avgs.mean()),
            exec_seconds=float(exec_secs.mean()),
            exec_cores=int(round(float(exec_cores_arr.mean()))),
            exec_mem_gb=float(exec_mems.mean()),
            dynamic_allocation=dynamic_allocation,
            duration_std_s=float(durs.std()),
            exec_active_std_s=float(actives.std()),
            overhead_std_s=float((durs - actives).std()),
            contention_tax_std_s=float(contentions.std()),
            resource_wait_std_s=float(res_waits.std()),
            # Startup = the part of overhead that is NOT resource wait. Its per-run spread is
            # the std of max(0, duration − active − resource_wait), the same quantity the row
            # renders from the grouped means, so the ± lines up with the printed value.
            planning_startup_std_s=float(np.maximum(0.0, durs - actives - res_waits).std()),
            bytes_read_std=float(reads.std()),
            bytes_written_std=float(writes.std()),
            cpu_time_ms_std=float(cpus.std()),
            start_time=min(starts) if starts else "",
            end_time=max(ends) if ends else "",
        ))

    # Compute CV of bytes_read within each multi-run group
    multi = {k: v for k, v in groups.items() if len(v) > 1}
    cvs = []
    for members in multi.values():
        arr = np.array([j.bytes_read for j in members])
        mean = arr.mean()
        if mean > 0:
            cvs.append(arr.std() / mean)
        else:
            cvs.append(0.0)

    quality: dict = {
        "total_groups": len(groups),
        "single_run": len(groups) - len(multi),
        "multi_run": len(multi),
    }
    if cvs:
        cvs_arr = np.array(cvs)
        n = len(cvs_arr)
        quality.update({
            "cv_analyzed": n,
            "cv_homogeneous": int((cvs_arr < 0.1).sum()),
            "cv_moderate": int(((cvs_arr >= 0.1) & (cvs_arr < 0.5)).sum()),
            "cv_high": int(((cvs_arr >= 0.5) & (cvs_arr < 1.0)).sum()),
            "cv_problematic": int((cvs_arr > 1.0).sum()),
            "cv_median": float(np.median(cvs_arr)),
            "cv_p95": float(np.percentile(cvs_arr, 95)),
        })

    return grouped, quality


# ─── Scoring ───────────────────────────────────────────────────────────────────


def _score_jobs(
    jobs: list[RankedJob],
    ref_data: float = REF_DATA_BYTES,
    ref_bw: float = REF_INTENSITY_BPS,
    ref_files: float = REF_FILE_COUNT,
) -> list[RankedJob]:
    if not jobs:
        return jobs

    for job in jobs:
        total_bytes = job.bytes_read + job.bytes_written
        # Intensity is the combined I/O rate over the ACTIVE window (bytes actually moved
        # per second while the job runs) — the bandwidth-stress axis. Matches
        # read_intensity_bps + write_intensity_bps in the JSON output.
        intensity_bps = total_bytes / job.exec_active_s if job.exec_active_s > 0 else 0.0
        d_score = min(100.0, total_bytes / ref_data * 100)
        i_score = min(100.0, intensity_bps / ref_bw * 100)
        f_score = min(100.0, job.file_count / ref_files * 100)
        score = d_score + i_score + f_score

        if d_score >= 100 or i_score >= 100 or f_score >= 100:
            risk = "High"
        elif score > 100:
            risk = "Medium"
        else:
            risk = "Low"

        why = ""
        if risk == "High":
            dims = []
            if d_score >= 100:
                dims.append("Data")
            if i_score >= 100:
                dims.append("BW")
            if f_score >= 100:
                dims.append("Files")
            why = "+".join(dims)
        elif risk == "Medium":
            dominant = max(
                ("Data", d_score), ("BW", i_score), ("Files", f_score),
                key=lambda x: x[1],
            )
            why = dominant[0]

        job.score = round(score, 1)
        job.data_score = round(d_score, 1)
        job.intensity_score = round(i_score, 1)
        job.file_score = round(f_score, 1)
        job.risk = risk
        job.why = why

    jobs.sort(key=lambda j: j.score)
    return jobs


# ─── Formatting ────────────────────────────────────────────────────────────────


def _fmt_duration(s: float) -> str:
    if s < 60:
        return f"{s:.1f} s"
    elif s < 3600:
        return f"{s / 60:.1f} m"
    else:
        return f"{s / 3600:.1f} h"


def _fmt_bytes(b: float) -> str:
    if b < 1e6:
        return f"{b / 1e3:.0f} KB"
    elif b < 1e9:
        return f"{b / 1e6:.2f} MB"
    elif b < 1e12:
        return f"{b / 1e9:.2f} GB"
    else:
        return f"{b / 1e12:.2f} TB"


def _fmt_intensity(bps: float) -> str:
    gbps = bps * 8 / 1e9
    if gbps >= 1.0:
        return f"{gbps:.1f} Gbit/s"
    mbps = bps * 8 / 1e6
    if mbps >= 1.0:
        return f"{mbps:.0f} Mbit/s"
    kbps = bps * 8 / 1e3
    return f"{kbps:.0f} Kbit/s"


def _fmt_dur_c(s: float) -> str:
    """Compact duration (no space) for the mean ± std grouped columns."""
    if s < 60:
        return f"{s:.0f}s"
    if s < 3600:
        return f"{s / 60:.1f}m"
    return f"{s / 3600:.1f}h"


def _fmt_bytes_c(b: float) -> str:
    """Compact byte count (no space) for the mean ± std grouped columns."""
    if b < 1e6:
        return f"{b / 1e3:.0f}KB"
    if b < 1e9:
        return f"{b / 1e6:.0f}MB"
    if b < 1e12:
        return f"{b / 1e9:.1f}GB"
    return f"{b / 1e12:.2f}TB"


def _fmt_cpu_c(cpu_sec: float) -> str:
    """Compact CPU-seconds for the mean ± std grouped column."""
    if cpu_sec < 1e3:
        return f"{cpu_sec:.0f}"
    if cpu_sec < 1e6:
        return f"{cpu_sec / 1e3:.1f}k"
    return f"{cpu_sec / 1e6:.1f}M"


def _fmt_cpu_intensity(cores: float) -> str:
    """CPU-seconds consumed per wall-second (≈ mean cores busy over the run)."""
    return f"{cores:.1f}×"


def _fmt_exec(avg: float, peak: int) -> str:
    """Mean concurrent executors / peak — the churn-proof 'executors used'."""
    return f"{avg:.0f}/{peak:d}"


def _fmt_alloc(dyn: bool | None) -> str:
    """Allocation mode from spark.dynamicAllocation.enabled."""
    if dyn is None:
        return "?"
    return "dyn" if dyn else "static"


def _fmt_exec_size(cores: int, mem_gb: float) -> str:
    """Per-executor resource shape: cores + memory (memory '?' when not recorded)."""
    if cores <= 0:
        return "—"
    mem = f"{mem_gb:.0f}GB" if mem_gb > 0 else "?"
    return f"{cores}c/{mem}"


def _observed_range(jobs: list[RankedJob]) -> tuple[str, str]:
    """Earliest start and latest end across jobs (ISO-8601, lexical order). Empty if unknown.

    ISO-8601 UTC timestamps sort lexically, so min/max on the strings is the true range
    without parsing.
    """
    starts = [j.start_time for j in jobs if j.start_time]
    ends = [j.end_time for j in jobs if j.end_time]
    return (min(starts) if starts else "", max(ends) if ends else "")


def _fmt_span(from_iso: str, to_iso: str) -> str:
    """Human span between two ISO-8601 timestamps, e.g. '18.0 days'. Empty on parse failure."""
    try:
        a = datetime.fromisoformat(from_iso.replace("Z", "+00:00"))
        b = datetime.fromisoformat(to_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return ""
    secs = (b - a).total_seconds()
    if secs < 0:
        return ""
    if secs < 3600:
        return f"{secs / 60:.0f} min"
    if secs < 86400:
        return f"{secs / 3600:.1f} h"
    return f"{secs / 86400:.1f} days"


def _pm(fmt, mean: float, std: float, show: bool) -> str:
    """Render 'mean ± std' (or just mean when std is 0 / single run)."""
    if show and std > 0:
        return f"{fmt(mean)} ± {fmt(std)}"
    return fmt(mean)


def _format_text(jobs: list[RankedJob], engine: str, grouped: bool = False, quality: dict | None = None) -> str:
    lines = []
    show_runs = grouped or any(j.runs > 1 for j in jobs)
    # In grouped mode the four per-run columns carry "mean ± std", so they need to be
    # wider. The header notes the columns are per-run means.
    dw, bw = (16, 17) if grouped else (8, 8)
    iw = 15                          # read / write intensity columns
    cw = 16 if grouped else 9        # CPU-sec (mean ± std when grouped)
    ew = 11                          # Spark executors avg/peak column
    aw = 6                           # allocation mode (dyn/static)
    sw = 9                           # per-executor size (cores/mem)
    is_spark = engine == "spark"     # resource columns are Spark-only for now
    res_hdr = (f"{'Exec a/pk':>{ew}}  {'Alloc':>{aw}}  {'Exec size':>{sw}}  " if is_spark else "")
    # Contention = executor-acquisition wait (Spark only); sits inside Exec, unlike Overhead.
    con_hdr = (f"{'Contention':>{dw}}  " if is_spark else "")
    # Spark splits overhead into its two parts so the row partitions cleanly:
    # Exec + Res wait + Startup = Duration. Res wait is the pre-executor wait (would vanish
    # with capacity); Startup is planning/driver/tail. Impala keeps one Overhead column
    # (always ~0 there, since active == duration).
    ovh_hdr = (f"{'Res wait':>{dw}}  {'Startup':>{dw}}  " if is_spark else f"{'Overhead':>{dw}}  ")
    dur_hdr = "Duration~" if grouped else "Duration"  # ~ marks "per-run mean ± std"
    # In grouped mode each row is one recurrence group, so the last column is the
    # grouping key (the normalized match_id / SQL pattern), not a single job's name.
    name_hdr = "Pattern" if grouped else "Name"
    header = (
        f"{'Rank':>4}  {'Score':>5}  {'Data%':>5}  {'BW%':>5}  {'File%':>5}  "
        f"{dur_hdr:>{dw}}  {'Exec':>{dw}}  {ovh_hdr}{con_hdr}"
        f"{'Read':>{bw}}  {'Read Intensity':>{iw}}  {'Written':>{bw}}  {'Write Intensity':>{iw}}  "
        f"{'CPU-s':>{cw}}  {'CPU Intensity':>{iw}}  {res_hdr}{'Files':>6}  "
        + (f"{'Runs':>5}  " if show_runs else "")
        + f"{'Risk':>6}  {'Why':>9}  {'Type':>5}  {name_hdr}"
    )
    sep = "─" * len(header)
    lines.append(header)
    lines.append(sep)

    for i, job in enumerate(jobs, 1):
        # Intensities are rates over the ACTIVE execution window (stages actually
        # running), not wall-clock — wall-clock includes admission/queue/idle time,
        # which would understate how hard the job worked while it was running.
        act = job.exec_active_s
        rd_int = job.bytes_read / act if act > 0 else 0.0
        wr_int = job.bytes_written / act if act > 0 else 0.0
        cpu_int = (job.cpu_time_ms / 1000) / act if act > 0 else 0.0
        # Overhead = wall-clock not spent executing (admission/queue, planning, startup,
        # client fetch). Its ± is the std of per-run (duration − active), from _group_jobs.
        # For Spark it splits into Res wait (pre-executor) + Startup (the rest); Impala shows
        # the single Overhead. Exec + Res wait + Startup = Duration for both engines.
        ovh_s = max(0.0, job.duration_s - job.exec_active_s)
        rw_s = job.resource_wait_s
        startup_s = max(0.0, ovh_s - rw_s)
        if grouped:
            pm = job.runs > 1
            dur = _pm(_fmt_dur_c, job.duration_s, job.duration_std_s, pm)
            exe = _pm(_fmt_dur_c, job.exec_active_s, job.exec_active_std_s, pm)
            ovh = _pm(_fmt_dur_c, ovh_s, job.overhead_std_s, pm)
            rw = _pm(_fmt_dur_c, rw_s, job.resource_wait_std_s, pm)
            stp = _pm(_fmt_dur_c, startup_s, job.planning_startup_std_s, pm)
            con = _pm(_fmt_dur_c, job.contention_tax_s, job.contention_tax_std_s, pm)
            rd = _pm(_fmt_bytes_c, job.bytes_read, job.bytes_read_std, pm)
            wr = _pm(_fmt_bytes_c, job.bytes_written, job.bytes_written_std, pm)
            cpu = _pm(_fmt_cpu_c, job.cpu_time_ms / 1000, job.cpu_time_ms_std / 1000, pm)
        else:
            dur, exe = _fmt_duration(job.duration_s), _fmt_duration(job.exec_active_s)
            ovh = _fmt_duration(ovh_s)
            rw = _fmt_duration(rw_s)
            stp = _fmt_duration(startup_s)
            con = _fmt_duration(job.contention_tax_s)
            rd, wr = _fmt_bytes(job.bytes_read), _fmt_bytes(job.bytes_written)
            cpu = _fmt_cpu_c(job.cpu_time_ms / 1000)
        res = ""
        if is_spark:
            res = (f"{_fmt_exec(job.exec_avg, job.exec_peak):>{ew}}  "
                   f"{_fmt_alloc(job.dynamic_allocation):>{aw}}  "
                   f"{_fmt_exec_size(job.exec_cores, job.exec_mem_gb):>{sw}}  ")
        con_cell = f"{con:>{dw}}  " if is_spark else ""
        ovh_cell = (f"{rw:>{dw}}  {stp:>{dw}}  " if is_spark else f"{ovh:>{dw}}  ")
        row = (
            f"{i:>4}  {job.score:>5.0f}  {job.data_score:>5.0f}  "
            f"{job.intensity_score:>5.0f}  {job.file_score:>5.0f}  "
            f"{dur:>{dw}}  {exe:>{dw}}  {ovh_cell}{con_cell}"
            f"{rd:>{bw}}  {_fmt_intensity(rd_int):>{iw}}  "
            f"{wr:>{bw}}  {_fmt_intensity(wr_int):>{iw}}  "
            f"{cpu:>{cw}}  {_fmt_cpu_intensity(cpu_int):>{iw}}  {res}{job.file_count:>6}  "
            + (f"{job.runs:>5}  " if show_runs else "")
            + f"{job.risk:>6}  {job.why:>9}  {job.job_type or '—':>5}  {job.name}"
        )
        lines.append(row)

    lines.append(sep)
    lines.append("")

    buckets = {"Low": [], "Medium": [], "High": []}
    for j in jobs:
        buckets[j.risk].append(j)

    total_runs = sum(j.runs for j in jobs)
    lines.append(f"── Summary ({engine}) ──")
    range_from, range_to = _observed_range(jobs)
    if range_from and range_to:
        span = _fmt_span(range_from, range_to)
        span_str = f"  ({span})" if span else ""
        lines.append(f"  Data range:      {range_from} → {range_to}{span_str}")
    if grouped:
        lines.append(f"  Unique queries:  {len(jobs):>6}  (from {total_runs} executions)")
    else:
        lines.append(f"  Jobs scored:     {len(jobs):>6}")
    for risk in ("Low", "Medium", "High"):
        bucket = buckets[risk]
        count = len(bucket)
        pct = count * 100 / len(jobs) if jobs else 0
        read = sum(j.bytes_read for j in bucket)
        written = sum(j.bytes_written for j in bucket)
        runs_str = f"  runs={sum(j.runs for j in bucket)}" if grouped else ""
        lines.append(f"  {risk + ' risk:':<15}{count:>6} ({pct:.1f}%)  "
                     f"read={_fmt_bytes(read)}  written={_fmt_bytes(written)}{runs_str}")
    total_read = sum(j.bytes_read for j in jobs)
    total_written = sum(j.bytes_written for j in jobs)
    total_runs_str = f"  runs={total_runs}" if grouped else ""
    lines.append(f"  {'Total:':<15}{len(jobs):>6} (100.0%)  "
                 f"read={_fmt_bytes(total_read)}  written={_fmt_bytes(total_written)}{total_runs_str}")
    lines.append(f"  Reference:       3 GB data | 1 Gbit/s bandwidth | 1000 files")

    total_cpu_ms = sum(j.cpu_time_ms for j in jobs)
    if total_cpu_ms > 0:
        total_cpu_sec = total_cpu_ms / 1000
        lines.append("")
        lines.append(f"── Capacity Offload ──")
        lines.append(f"  Total CPU-time:  {total_cpu_sec:>12,.0f} CPU-sec  ({total_cpu_sec/3600:.1f} CPU-hours)")
        for risk in ("Low", "Medium", "High"):
            bucket = buckets[risk]
            cpu_sec = sum(j.cpu_time_ms for j in bucket) / 1000
            pct = cpu_sec * 100 / total_cpu_sec if total_cpu_sec > 0 else 0
            lines.append(f"  {risk + ' risk:':<15}{cpu_sec:>12,.0f} CPU-sec  ({pct:.1f}%)")
        low_pct = sum(j.cpu_time_ms for j in buckets["Low"]) * 100 / total_cpu_ms if total_cpu_ms > 0 else 0
        lines.append(f"  Offloadable:     {low_pct:.1f}% of on-prem compute can be freed by moving Low-risk jobs to cloud")

    if grouped and quality:
        n = quality.get("cv_analyzed", 0)
        single_execs = quality["single_run"]
        multi_execs = total_runs - single_execs
        lines.append("")
        lines.append(f"── Grouping Quality ──")
        lines.append(f"  Executions:      {total_runs:>6}")
        groups_w = "group" if single_execs == 1 else "groups"
        patterns_w = "pattern" if quality['multi_run'] == 1 else "patterns"
        lines.append(f"    One-off:       {single_execs:>6} ({single_execs*100//total_runs}%)  "
                     f"ran once → {single_execs} {groups_w}")
        lines.append(f"    Recurring:     {multi_execs:>6} ({multi_execs*100//total_runs}%)  "
                     f"grouped into {quality['multi_run']} {patterns_w}")
        if n:
            homo = quality["cv_homogeneous"]
            mod = quality["cv_moderate"]
            high = quality["cv_high"]
            prob = quality["cv_problematic"]
            lines.append(f"  Grouping accuracy (CV of bytes_read per pattern):")
            lines.append(f"    CV < 0.1:      {homo:>5} ({homo*100/n:.1f}%)  homogeneous")
            lines.append(f"    CV 0.1–0.5:    {mod:>5} ({mod*100/n:.1f}%)  moderate variance")
            lines.append(f"    CV 0.5–1.0:    {high:>5} ({high*100/n:.1f}%)  high variance")
            lines.append(f"    CV > 1.0:      {prob:>5} ({prob*100/n:.1f}%)  problematic")
            lines.append(f"  Median CV: {quality['cv_median']:.4f}  P95: {quality['cv_p95']:.3f}")
        lines.append(f"  Method: literal normalization (strings/numbers → ?, table names preserved)")

    return "\n".join(lines)


def _to_json(jobs: list[RankedJob], engine: str, grouped: bool = False) -> dict:
    low = sum(1 for j in jobs if j.risk == "Low")
    med = sum(1 for j in jobs if j.risk == "Medium")
    high = sum(1 for j in jobs if j.risk == "High")

    summary = {
        "total": len(jobs),
        "low_risk": low,
        "medium_risk": med,
        "high_risk": high,
        "total_bytes_read": int(sum(j.bytes_read for j in jobs)),
        "total_bytes_written": int(sum(j.bytes_written for j in jobs)),
    }
    if grouped:
        summary["total_executions"] = sum(j.runs for j in jobs)

    obs_from, obs_to = _observed_range(jobs)

    def _job_dict(i: int, job: RankedJob) -> dict:
        # Intensities are rates over the active execution window, matching the text table.
        # Emitted in BITS per second (× 8) so "_bps" reads as bits and lines up with WAN
        # link capacity (Gbps); internal scoring stays in bytes/s (scores unaffected).
        act = job.exec_active_s
        d = {
            "rank": i + 1,
            "name": job.name,
            "type": job.job_type,
            "score": job.score,
            "risk": job.risk,
            "why": job.why,
            "runs": job.runs,
            "duration_s": round(job.duration_s, 1),
            "duration_std_s": round(job.duration_std_s, 1),
            "exec_active_s": round(job.exec_active_s, 1),
            "exec_active_std_s": round(job.exec_active_std_s, 1),
            "overhead_s": round(max(0.0, job.duration_s - job.exec_active_s), 1),
            "overhead_std_s": round(job.overhead_std_s, 1),
            "bytes_read": int(job.bytes_read),
            "bytes_read_std": int(job.bytes_read_std),
            "bytes_written": int(job.bytes_written),
            "bytes_written_std": int(job.bytes_written_std),
            "read_intensity_bps": round(job.bytes_read * 8 / act, 1) if act > 0 else 0.0,
            "write_intensity_bps": round(job.bytes_written * 8 / act, 1) if act > 0 else 0.0,
            "cpu_time_ms": job.cpu_time_ms,
            "cpu_intensity_cores": round((job.cpu_time_ms / 1000) / act, 3) if act > 0 else 0.0,
            "file_count": job.file_count,
            "data_score": job.data_score,
            "intensity_score": job.intensity_score,
            "file_score": job.file_score,
        }
        # Executor-acquisition wait (Spark only). Inside exec_active_s, distinct from
        # overhead_s. Omitted for Impala (no executor concept; the live path can't derive it).
        if engine == "spark":
            d["resource_wait_s"] = round(job.resource_wait_s, 1)
            d["resource_wait_std_s"] = round(job.resource_wait_std_s, 1)
            # Overhead minus resource wait = planning/startup/tail. Emitted so a reader has
            # the full 3-way split (resource_wait + planning + active = wall-clock) without
            # recomputing it. Clamped ≥ 0 to match the parser's clamp.
            d["planning_startup_s"] = round(max(0.0, d["overhead_s"] - d["resource_wait_s"]), 1)
            d["contention_tax_s"] = round(job.contention_tax_s, 1)
            d["contention_tax_std_s"] = round(job.contention_tax_std_s, 1)
        # Spark resource footprint (engine-specific; omitted for engines without executors).
        if engine == "spark":
            d["resources"] = {
                "dynamic_allocation": job.dynamic_allocation,
                "executor_peak": job.exec_peak,
                "executor_avg": round(job.exec_avg, 1),
                "executor_cores": job.exec_cores,
                "executor_memory_gb": job.exec_mem_gb,
                "executor_seconds": round(job.exec_seconds, 1),
                "vcore_hours": round(job.exec_seconds * job.exec_cores / 3600, 2),
            }
        return d

    return {
        "engine": engine,
        "data_range": {"from": obs_from, "to": obs_to},
        "grouped": grouped,
        "reference": {
            "data_bytes": REF_DATA_BYTES,
            "intensity_bps": REF_INTENSITY_BPS * 8,   # bits/s (1 Gbps); scoring uses bytes/s internally
            "file_count": REF_FILE_COUNT,
        },
        "summary": summary,
        "jobs": [_job_dict(i, job) for i, job in enumerate(jobs)],
    }


# ─── Main ─────────────────────────────────────────────────────────────────────


def rank(
    engine: str,
    ref_data: float = REF_DATA_BYTES,
    ref_bw: float = REF_INTENSITY_BPS,
    ref_files: float = REF_FILE_COUNT,
    group: bool = False,
) -> tuple[list[RankedJob], dict]:
    """Rank the processed workloads for the engine.

    Reads ``data/processed/<engine>``. Returns (scored list sorted by score
    ascending, quality stats).
    """
    jobs = _load_source(f"data/processed/{engine}", engine)

    quality = {}
    if group:
        jobs, quality = _group_jobs(jobs)

    return _score_jobs(jobs, ref_data=ref_data, ref_bw=ref_bw, ref_files=ref_files), quality


def main():
    parser = argparse.ArgumentParser(
        description="Rank workloads by disaggregation sensitivity"
    )
    parser.add_argument(
        "--engine", required=True, choices=["impala", "spark"],
        help="Engine to rank (impala or spark)",
    )
    parser.add_argument("--ref-data", type=float, default=REF_DATA_BYTES,
                        help="Reference data volume in bytes (default: 3 GB)")
    parser.add_argument("--ref-gbps", type=float, default=REF_INTENSITY_BPS * 8 / 1e9,
                        help="Reference bandwidth in Gbit/s (default: 1)")
    parser.add_argument("--ref-files", type=float, default=REF_FILE_COUNT,
                        help="Reference file count (default: 1000)")
    parser.add_argument("--group", dest="group", action="store_true", default=True,
                        help="(default) group recurring queries by normalized SQL / match_id")
    parser.add_argument("--no-group", dest="group", action="store_false",
                        help="Rank individual executions without grouping")
    parser.add_argument("--json", action="store_true", help="Output as JSON to stdout")
    parser.add_argument("--save-json", dest="save_json", action="store_true",
                        help="Save JSON to data/ranking/<engine>_ranking.json (still prints the "
                             "text table unless --json is also given)")
    args = parser.parse_args()

    # CLI takes Gbit/s (matches WAN link specs & the bits-based JSON); scorer works in bytes/s.
    ref_bw_bytes = args.ref_gbps * 1e9 / 8
    jobs, quality = rank(args.engine,
                         ref_data=args.ref_data, ref_bw=ref_bw_bytes, ref_files=args.ref_files,
                         group=args.group)

    if args.save_json:
        payload = _to_json(jobs, args.engine, grouped=args.group)
        out_dir = Path("data/ranking")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{args.engine}_ranking.json"
        out_path.write_text(json.dumps(payload, indent=2))
        print(f"Wrote ranking JSON to {out_path}", file=sys.stderr)

    if args.json:
        print(json.dumps(_to_json(jobs, args.engine, grouped=args.group), indent=2))
    else:
        print(_format_text(jobs, args.engine, grouped=args.group, quality=quality))


if __name__ == "__main__":
    main()
