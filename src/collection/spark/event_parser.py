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

"""Pure parsing of Spark event logs (NDJSON) into stage and job summaries.

No I/O — accepts pre-parsed JSON dicts and returns dataclass results.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

_INTERNAL_METRICS = {
    "internal.metrics.executorRunTime": "executor_runtime_ms",
    "internal.metrics.executorCpuTime": "executor_cpu_time_ns",
    "internal.metrics.input.bytesRead": "input_bytes",
    "internal.metrics.output.bytesWritten": "output_bytes",
    "internal.metrics.shuffle.write.bytesWritten": "shuffle_write_bytes",
    "internal.metrics.shuffle.read.remoteBytesRead": "shuffle_read_remote_bytes",
    "internal.metrics.shuffle.read.localBytesRead": "shuffle_read_local_bytes",
}

_FILE_READ_NAMES = {"number of files read"}
_PARTITION_READ_NAMES = {"number of partitions read"}

_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")
_TABLE_RE = re.compile(
    r"(?:Scan|FileScan)\s+(?:parquet|orc|json|csv|hive|delta)\s+([`\w.]+)"
)


# query_id is the per-run key (= app_id); match_id is the recurrence-group key that
# folds repeated runs of the same logical job together.
@dataclass
class StageSummary:
    query_id: str
    app_id: str
    app_name: str
    match_id: str
    type: str
    stage_id: int
    total_stages: int
    stage_duration_s: float
    num_tasks: int
    avg_task_duration_s: float
    executor_runtime_s: float
    executor_cpu_time_s: float
    input_read_gb: float
    output_write_gb: float
    shuffle_read_gb: float
    shuffle_write_gb: float
    number_of_files_read: Optional[int]
    static_number_of_files_read: Optional[int]
    number_of_partitions_read: Optional[int]
    spark_num_executors: int
    spark_executor_cores: int
    spark_executor_memory_gb: float
    spark_avg_executors: float
    spark_executor_seconds: float
    spark_dynamic_allocation: Optional[bool]


@dataclass
class JobSummary:
    query_id: str
    app_id: str
    app_name: str
    match_id: str
    type: str
    spark_user: str
    spark_version: str
    spark_num_executors: int
    spark_executor_cores: int
    spark_executor_memory_gb: float
    spark_avg_executors: float
    spark_executor_seconds: float
    spark_dynamic_allocation: Optional[bool]
    start_time: str
    end_time: str
    exec_time_s: float
    active_exec_time_s: float
    # Wall-clock time before the first executor existed AND before execution began — the
    # app was admitted but no resource had arrived, so nothing could compute. This is the
    # slice of wall-clock that would collapse toward zero given enough cluster capacity.
    # Defined as (first_executor_ts − start_ts), CLAMPED to overhead so the three time
    # buckets partition wall-clock exactly:
    #     exec_time_s = resource_wait_s + planning/startup/tail + active_exec_time_s
    # where planning/startup/tail = overhead − resource_wait_s and overhead = exec_time_s
    # − active_exec_time_s. The clamp matters under dynamic allocation: there the first
    # stage is often submitted BEFORE the first executor arrives, so part of the resource
    # wait falls INSIDE active_exec_time_s — that in-active part is already surfaced by
    # contention_tax_s, and the clamp keeps it from being double-counted here. 0.0 when no
    # ExecutorAdded event was seen (truncated log, or an app that never got a slot): we do
    # not claim the whole wall-clock as resource wait on apps we cannot explain.
    resource_wait_s: float
    # Executor-acquisition wait summed across stages: Σ max(0, firstTaskLaunch − submission).
    # Time tasks spent pending for an executor slot (dynamic-allocation cold-start / slot
    # contention). Lives inside active_exec_time_s, so it is distinct from overhead. It
    # overlaps resource_wait_s only conceptually: resource_wait_s is the pre-active slice
    # (outside active), this is the in-active slice — the two never cover the same second.
    contention_tax_s: float
    num_spark_stages: int
    spark_total_executor_runtime_s: float
    spark_total_executor_cpu_time_s: float
    spark_total_input_read_gb: float
    spark_total_output_write_gb: float
    spark_total_shuffle_read_gb: float
    spark_total_shuffle_write_gb: float
    total_files_read: Optional[int]


class _ParserState:
    def __init__(self) -> None:
        self.app_id: str = ""
        self.app_name: str = ""
        self.spark_user: str = ""
        self.spark_version: str = ""
        self.start_ts: int = 0
        self.end_ts: int = 0

        self.executor_cores: int = 0
        self.executor_memory: str = ""
        # spark.dynamicAllocation.enabled from the environment update: True (elastic),
        # False (static/fixed instances), or None when the property isn't recorded.
        self.dynamic_allocation: Optional[bool] = None
        # Peak concurrent executors. Under dynamic allocation a job churns executors
        # (add/remove repeatedly), so the count of distinct IDs ever seen wildly
        # overstates cluster width. Track the live set and record its high-water mark.
        self._live_executors: set[str] = set()
        self._peak_executors: int = 0
        # Time-integral of concurrent executors: ∫ live_executor_count dt (executor-seconds).
        # Under dynamic allocation the count churns, so this integral (÷ duration → mean
        # concurrent executors) is the churn-proof "resources used" figure. We accrue the
        # area of each [last change, this change] interval at the count held during it.
        self._executor_seconds: float = 0.0
        self._last_exec_ts: int = 0
        # Timestamp of the first non-driver ExecutorAdded. Until an executor exists no task
        # can run, so [start_ts, first executor] is pure resource-acquisition wait. Becomes
        # the resource_wait_s bucket at finalize (clamped to overhead). See that field.
        self._first_executor_ts: int = 0

        self.stages: dict[int, dict[str, Any]] = {}
        # Earliest task Launch Time per stage id (from SparkListenerTaskStart). The gap
        # to the stage's Submission Time is executor-acquisition wait: tasks were pending
        # because no executor slot was available yet (dynamic-allocation cold-start /
        # slot contention). This wait sits INSIDE the active-execution span, so it is not
        # captured by overhead (duration − active); contention_tax_s surfaces it.
        self._stage_first_task_ts: dict[int, int] = {}

        self._is_sql: bool = False
        self._sql_descriptions: list[str] = []
        self._tables: set[str] = set()
        self._file_read_acc_ids: set[int] = set()
        self._partition_read_acc_ids: set[int] = set()
        self._acc_values: dict[int, int] = defaultdict(int)


def _normalize_name(name: str) -> str:
    return _NORMALIZE_RE.sub("_", name.lower()).strip("_")


# Volatile tokens that change every run and would otherwise give each run a unique
# fingerprint, defeating recurrence grouping. Production pipelines materialize
# batch-versioned staging tables (foo_20948 -> foo_20949) and Structured Streaming
# descriptions embed a per-run runId/batch. Normalize these away before hashing.
_TABLE_BATCH_SUFFIX_RE = re.compile(r"_\d+$")
_DESC_VOLATILE_SUBS = (
    (re.compile(r"runId\s*=\s*[0-9a-fA-F-]{36}"), "runId = X"),
    (re.compile(r"batch\s*=\s*\d+"), "batch = N"),
)


def _normalize_table(table: str) -> str:
    """Strip a trailing _<digits> batch/version suffix from the table name so
    versioned staging tables group across runs (account_detail_20948 -> account_detail)."""
    return _TABLE_BATCH_SUFFIX_RE.sub("", table)


def _normalize_desc(desc: str) -> str:
    """Replace per-run streaming tokens (runId, batch) with placeholders, keeping the
    stable streaming query id so distinct streaming queries stay distinguishable."""
    for rx, repl in _DESC_VOLATILE_SUBS:
        desc = rx.sub(repl, desc)
    return desc


def _sql_fingerprint(tables: set[str], descriptions: list[str]) -> str:
    if tables:
        key = "|".join(sorted(_normalize_table(t) for t in tables))
    else:
        key = "|".join(sorted(set(_normalize_desc(d) for d in descriptions)))
    return hashlib.sha256(key.encode()).hexdigest()[:8]


def _bytes_to_gb(b: float) -> float:
    return b / (1024 ** 3)


_MEMORY_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*$", re.IGNORECASE)
# Spark memory strings use binary units; a bare number defaults to MiB (Spark convention).
_MEMORY_FACTOR_GB = {"": 1 / 1024, "k": 1 / 1024 ** 2, "m": 1 / 1024, "g": 1.0, "t": 1024.0}


def _parse_memory_gb(mem: str) -> float:
    m = _MEMORY_RE.match(mem or "")
    if not m:
        return 0.0
    return round(float(m.group(1)) * _MEMORY_FACTOR_GB[m.group(2).lower()], 6)


def _is_scan_stage(stage: dict[str, Any]) -> bool:
    """A stage that reads external input and does no shuffle read — a pure scan.

    Its ``num_tasks`` is the RDD-side proxy for splits/files touched: this is the
    only per-stage file signal available without replaying per-task events.
    """
    return stage["input_bytes"] > 0 and (
        stage["shuffle_read_remote_bytes"] + stage["shuffle_read_local_bytes"]
    ) == 0


def _walk_plan_for_accumulators(node: Any, state: _ParserState) -> None:
    if not isinstance(node, dict):
        return
    simple_str = node.get("simpleString", "")
    match = _TABLE_RE.search(simple_str)
    if match:
        table_name = match.group(1).strip("`")
        state._tables.add(table_name)
    for metric in node.get("metrics", []):
        try:
            acc_id = int(metric["accumulatorId"])
            mname = metric.get("name", "").strip().lower()
        except (KeyError, ValueError, TypeError):
            continue
        if mname in _FILE_READ_NAMES:
            state._file_read_acc_ids.add(acc_id)
        elif mname in _PARTITION_READ_NAMES:
            state._partition_read_acc_ids.add(acc_id)
    for child in node.get("children", []):
        _walk_plan_for_accumulators(child, state)


def _handle_log_start(event: dict, state: _ParserState) -> None:
    state.spark_version = event.get("Spark Version", "")


def _handle_application_start(event: dict, state: _ParserState) -> None:
    state.app_id = event.get("App ID", "")
    state.app_name = event.get("App Name", "")
    state.spark_user = event.get("User", "")
    state.start_ts = event.get("Timestamp", 0)


def _handle_application_end(event: dict, state: _ParserState) -> None:
    state.end_ts = event.get("Timestamp", 0)


def _handle_environment_update(event: dict, state: _ParserState) -> None:
    raw = event.get("Spark Properties", {})
    if isinstance(raw, list):
        props = {p[0]: p[1] for p in raw if isinstance(p, (list, tuple)) and len(p) == 2}
    elif isinstance(raw, dict):
        props = raw
    else:
        return
    state.executor_memory = props.get("spark.executor.memory", "") or state.executor_memory
    enabled = props.get("spark.dynamicAllocation.enabled")
    if enabled is not None:
        state.dynamic_allocation = str(enabled).strip().lower() == "true"


def _handle_resource_profile_added(event: dict, state: _ParserState) -> None:
    try:
        cores = event["Executor Resource Requests"]["cores"]["Amount"]
        state.executor_cores = int(cores)
    except (KeyError, ValueError, TypeError):
        pass


def _accrue_executor_seconds(state: _ParserState, ts: int) -> None:
    """Credit the interval [last change, ts] with the executor count currently live,
    then advance the marker. Call BEFORE mutating the live set so the interval that just
    ended is attributed to the count that was held during it."""
    if state._last_exec_ts and ts >= state._last_exec_ts:
        state._executor_seconds += len(state._live_executors) * (ts - state._last_exec_ts) / 1000.0
    if ts:
        state._last_exec_ts = ts


def _handle_executor_added(event: dict, state: _ParserState) -> None:
    exec_id = event.get("Executor ID", "")
    if exec_id and exec_id != "driver":
        ts = event.get("Timestamp", 0)
        _accrue_executor_seconds(state, ts)
        state._live_executors.add(exec_id)
        state._peak_executors = max(state._peak_executors, len(state._live_executors))
        if state._first_executor_ts == 0 and ts:
            state._first_executor_ts = ts
    if state.executor_cores == 0:
        try:
            cores = event["Executor Info"]["Total Cores"]
            state.executor_cores = int(cores)
        except (KeyError, ValueError, TypeError):
            pass


def _handle_executor_removed(event: dict, state: _ParserState) -> None:
    _accrue_executor_seconds(state, event.get("Timestamp", 0))
    state._live_executors.discard(event.get("Executor ID", ""))


def _handle_task_start(event: dict, state: _ParserState) -> None:
    """Track the earliest task Launch Time per stage. Its gap to the stage Submission
    Time is the executor-acquisition wait (tasks pending, no slot yet)."""
    try:
        stage_id = int(event["Stage ID"])
        launch = int(event["Task Info"]["Launch Time"])
    except (KeyError, ValueError, TypeError):
        return
    if launch <= 0:
        return
    cur = state._stage_first_task_ts.get(stage_id)
    if cur is None or launch < cur:
        state._stage_first_task_ts[stage_id] = launch


def _handle_stage_completed(event: dict, state: _ParserState) -> None:
    try:
        info = event["Stage Info"]
        stage_id = int(info["Stage ID"])
    except (KeyError, ValueError, TypeError):
        return

    num_tasks = int(info.get("Number of Tasks", 0))
    submission_ts = info.get("Submission Time", 0)
    completion_ts = info.get("Completion Time", 0)
    duration_ms = max(0, completion_ts - submission_ts) if submission_ts and completion_ts else 0

    metrics: dict[str, float] = {
        "executor_runtime_ms": 0,
        "executor_cpu_time_ns": 0,
        "input_bytes": 0,
        "output_bytes": 0,
        "shuffle_write_bytes": 0,
        "shuffle_read_remote_bytes": 0,
        "shuffle_read_local_bytes": 0,
    }

    for acc in info.get("Accumulables", []):
        name = acc.get("Name", "")
        mapped = _INTERNAL_METRICS.get(name)
        if mapped:
            try:
                metrics[mapped] = float(acc["Value"])
            except (KeyError, ValueError, TypeError):
                pass

    state.stages[stage_id] = {
        "stage_id": stage_id,
        "num_tasks": num_tasks,
        "duration_ms": duration_ms,
        "submission_ts": submission_ts,
        "completion_ts": completion_ts,
        **metrics,
    }


def _handle_sql_execution_start(event: dict, state: _ParserState) -> None:
    state._is_sql = True
    desc = event.get("description", "")
    if desc:
        state._sql_descriptions.append(desc)
    plan = event.get("sparkPlanInfo")
    if plan:
        _walk_plan_for_accumulators(plan, state)


def _handle_sql_adaptive_update(event: dict, state: _ParserState) -> None:
    plan = event.get("sparkPlanInfo")
    if plan:
        _walk_plan_for_accumulators(plan, state)


def _handle_driver_accum_updates(event: dict, state: _ParserState) -> None:
    for update in event.get("accumUpdates", []):
        try:
            if isinstance(update, (list, tuple)) and len(update) >= 2:
                acc_id = int(update[0])
                value = int(update[1])
                state._acc_values[acc_id] = value
        except (ValueError, TypeError):
            pass


_EVENT_HANDLERS = {
    "SparkListenerLogStart": _handle_log_start,
    "SparkListenerApplicationStart": _handle_application_start,
    "SparkListenerApplicationEnd": _handle_application_end,
    "SparkListenerEnvironmentUpdate": _handle_environment_update,
    "SparkListenerResourceProfileAdded": _handle_resource_profile_added,
    "SparkListenerExecutorAdded": _handle_executor_added,
    "SparkListenerExecutorRemoved": _handle_executor_removed,
    "SparkListenerTaskStart": _handle_task_start,
    "SparkListenerStageCompleted": _handle_stage_completed,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart": _handle_sql_execution_start,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate": _handle_sql_adaptive_update,
    "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates": _handle_driver_accum_updates,
}


def process_event(event: dict, state: _ParserState) -> None:
    handler = _EVENT_HANDLERS.get(event.get("Event", ""))
    if handler is not None:
        handler(event, state)


def _resolve_file_counts(state: _ParserState) -> tuple[Optional[int], Optional[int]]:
    if not state._is_sql:
        scan_tasks = sum(
            s["num_tasks"] for s in state.stages.values() if _is_scan_stage(s)
        )
        return (scan_tasks if scan_tasks > 0 else None, None)

    files_read = 0
    for acc_id in state._file_read_acc_ids:
        files_read += state._acc_values.get(acc_id, 0)

    partitions_read = 0
    for acc_id in state._partition_read_acc_ids:
        partitions_read += state._acc_values.get(acc_id, 0)

    return (
        files_read if files_read > 0 else None,
        partitions_read if partitions_read > 0 else None,
    )


def _compute_match_id(state: _ParserState) -> str:
    base = _normalize_name(state.app_name)
    if state._is_sql and (state._tables or state._sql_descriptions):
        fp = _sql_fingerprint(state._tables, state._sql_descriptions)
        return f"{base}_{fp}"
    return base


def _ts_to_iso(ts_ms: int) -> str:
    if ts_ms == 0:
        return ""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_results(state: _ParserState) -> tuple[JobSummary, list[StageSummary]]:
    match_id = _compute_match_id(state)
    job_type = "sql" if state._is_sql else "rdd"
    file_count, partition_count = _resolve_file_counts(state)

    num_executors = state._peak_executors
    memory_gb = _parse_memory_gb(state.executor_memory)
    total_stages = len(state.stages)

    # App wall-clock (ApplicationEnd - ApplicationStart); computed here (not just below)
    # so the executor-seconds integral can be closed to app end and averaged.
    app_duration_s = (state.end_ts - state.start_ts) / 1000.0 if state.end_ts > state.start_ts else 0.0
    # Close the executor-concurrency integral: credit the final interval (last executor
    # change → app end) with the still-live count, then derive the mean concurrency.
    if state._last_exec_ts and state.end_ts > state._last_exec_ts:
        state._executor_seconds += len(state._live_executors) * (state.end_ts - state._last_exec_ts) / 1000.0
    executor_seconds = round(state._executor_seconds, 3)
    avg_executors = round(executor_seconds / app_duration_s, 4) if app_duration_s > 0 else 0.0

    stage_summaries: list[StageSummary] = []
    total_runtime = 0.0
    total_cpu = 0.0
    total_input = 0.0
    total_output = 0.0
    total_shuffle_read = 0.0
    total_shuffle_write = 0.0

    for s in state.stages.values():
        runtime_s = s["executor_runtime_ms"] / 1000.0
        cpu_s = s["executor_cpu_time_ns"] / 1e9
        input_gb = _bytes_to_gb(s["input_bytes"])
        output_gb = _bytes_to_gb(s["output_bytes"])
        shuffle_read_gb = _bytes_to_gb(
            s["shuffle_read_remote_bytes"] + s["shuffle_read_local_bytes"]
        )
        shuffle_write_gb = _bytes_to_gb(s["shuffle_write_bytes"])
        duration_s = s["duration_ms"] / 1000.0
        num_tasks = s["num_tasks"]
        avg_task_s = runtime_s / num_tasks if num_tasks > 0 else 0.0

        # Per-stage file count: for RDD jobs, a pure-scan stage's task count is the
        # split/file proxy. SQL file counts live only in driver-level accumulators
        # (never in stage Accumulables), so they can't be attributed per stage
        # without replaying per-task events — left None here, accurate at job level.
        stage_files = num_tasks if (not state._is_sql and _is_scan_stage(s)) else None

        total_runtime += runtime_s
        total_cpu += cpu_s
        total_input += input_gb
        total_output += output_gb
        total_shuffle_read += shuffle_read_gb
        total_shuffle_write += shuffle_write_gb

        stage_summaries.append(StageSummary(
            query_id=state.app_id,
            app_id=state.app_id,
            app_name=state.app_name,
            match_id=match_id,
            type=job_type,
            stage_id=s["stage_id"],
            total_stages=total_stages,
            stage_duration_s=round(duration_s, 6),
            num_tasks=num_tasks,
            avg_task_duration_s=round(avg_task_s, 6),
            executor_runtime_s=round(runtime_s, 3),
            executor_cpu_time_s=round(cpu_s, 3),
            input_read_gb=round(input_gb, 6),
            output_write_gb=round(output_gb, 6),
            shuffle_read_gb=round(shuffle_read_gb, 6),
            shuffle_write_gb=round(shuffle_write_gb, 6),
            number_of_files_read=stage_files,
            static_number_of_files_read=None,
            number_of_partitions_read=None,
            spark_num_executors=num_executors,
            spark_executor_cores=state.executor_cores,
            spark_executor_memory_gb=memory_gb,
            spark_avg_executors=avg_executors,
            spark_executor_seconds=executor_seconds,
            spark_dynamic_allocation=state.dynamic_allocation,
        ))

    stage_summaries.sort(key=lambda s: s.stage_id)

    # exec_time_s is app wall-clock (ApplicationEnd - ApplicationStart): it includes
    # admission/queue wait and driver-side idle, so it is NOT pure compute time and can
    # far exceed total_executor_runtime_s. Use total_executor_runtime_s for the
    # parallel compute term; treat exec_time_s as the end-to-end serial+parallel total.
    # (duration_s computed above so the executor-seconds integral could be finalized.)

    # active_exec_time_s is the span during which stages were actually running:
    # last stage completion - first stage submission. Bounded by wall-clock (stages run
    # within the app lifetime), it strips pre-first-stage admission and post-last-stage
    # idle, so it reflects real execution time. Unlike summing stage durations it does not
    # double-count concurrent stages. Zero when no stage has valid timestamps.
    submissions = [s["submission_ts"] for s in state.stages.values() if s["submission_ts"]]
    completions = [s["completion_ts"] for s in state.stages.values() if s["completion_ts"]]
    active_s = (max(completions) - min(submissions)) / 1000.0 if submissions and completions else 0.0
    active_s = max(0.0, active_s)

    # Executor-acquisition wait: per stage, the gap between when it was submitted and when
    # its first task actually launched (an executor slot became available). Summed across
    # stages — an aggregate like the *_runtime totals, so it can exceed wall-clock when
    # stages wait concurrently. This is the contention that hides inside active_s.
    contention_ms = 0
    for s in state.stages.values():
        sub = s["submission_ts"]
        ft = state._stage_first_task_ts.get(s["stage_id"])
        if sub and ft and ft > sub:
            contention_ms += ft - sub
    contention_tax_s = round(contention_ms / 1000.0, 3)

    # Resource-acquisition wait: time from app start until the first executor arrived,
    # clamped to overhead (exec_time − active) so the three buckets partition wall-clock
    # exactly. Under dynamic allocation the first stage can be submitted before the first
    # executor, so the pre-active part goes here while the in-active part stays in
    # contention_tax_s — the clamp prevents double counting. 0 when no executor was ever
    # added (truncated log / never-scheduled app), so we never over-claim the wait.
    overhead_s = max(0.0, app_duration_s - active_s)
    if state._first_executor_ts and state.start_ts and state._first_executor_ts > state.start_ts:
        raw_wait_s = (state._first_executor_ts - state.start_ts) / 1000.0
        resource_wait_s = round(min(raw_wait_s, overhead_s), 3)
    else:
        resource_wait_s = 0.0

    job = JobSummary(
        query_id=state.app_id,
        app_id=state.app_id,
        app_name=state.app_name,
        match_id=match_id,
        type=job_type,
        spark_user=state.spark_user,
        spark_version=state.spark_version,
        spark_num_executors=num_executors,
        spark_executor_cores=state.executor_cores,
        spark_executor_memory_gb=memory_gb,
        spark_avg_executors=avg_executors,
        spark_executor_seconds=executor_seconds,
        spark_dynamic_allocation=state.dynamic_allocation,
        start_time=_ts_to_iso(state.start_ts),
        end_time=_ts_to_iso(state.end_ts),
        exec_time_s=round(app_duration_s, 3),
        active_exec_time_s=round(active_s, 3),
        resource_wait_s=resource_wait_s,
        contention_tax_s=contention_tax_s,
        num_spark_stages=total_stages,
        spark_total_executor_runtime_s=round(total_runtime, 3),
        spark_total_executor_cpu_time_s=round(total_cpu, 3),
        spark_total_input_read_gb=round(total_input, 6),
        spark_total_output_write_gb=round(total_output, 6),
        spark_total_shuffle_read_gb=round(total_shuffle_read, 6),
        spark_total_shuffle_write_gb=round(total_shuffle_write, 6),
        total_files_read=file_count,
    )

    return job, stage_summaries
