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

"""Extract transport-aware-model stage/job features from Spark event logs.

This is a self-contained parser that turns a generic Spark event log (NDJSON, as
served by the History Server) into the per-stage feature records the
``transport-aware-spark-model`` consumes — the same shape as its
``data/raw/stage_measurement_summary.json``. It does NOT need the bespoke
``SparkListener`` JAR the original project used; every value it emits is read
straight from the standard event stream.

Records are emitted PER RUN: ``query_id`` mirrors ``app_id`` (one Spark
application). Recurrence grouping and the on-prem<->cloud stage pairing
(``match_id``, the model's calibration target) are deliberately NOT done here.

Per-stage file and partition counts are SQL scan metrics that Spark reports once
per query (driver level), not per task. We attribute them to a stage the same way
``spark-exec-predictor``'s ``file_access.py`` does — a two-tier mapping:

  1. primary: an accumulator id -> stage from the per-task ``TaskEnd`` accumulables
     (used when a Spark version pushes the metric per task);
  2. fallback: the scan node's name -> stage, matched through each stage's
     ``RDD Info`` ``Scope`` name (used when the metric is driver-only).

The value itself comes from ``SparkListenerDriverAccumUpdates`` (and adaptive SQL
metric updates). ``number_of_files_read`` falls back to the scan-task-count proxy
for a pure-scan stage with no mapped count; anything unresolved stays ``None``.

Four inputs the model uses do not exist in any event log — they are OS/network
constants of the deployment, not per-job values:

    window_max_bytes  TCP window  (sysctl net.ipv4.tcp_rmem / net.core.rmem_max)
    mtu               W_init      (ip link show <iface>)
    network_latency_ms  RTT       (ping to the cloud side, or the link spec)
    tsg_max_bw_gbs    B_link      (WAN interconnect capacity, in Gb/s)

They are ``None`` unless supplied through :class:`EnvConstants`; nothing is ever
fabricated. See ``docs/spark_model_features.md``.

Job wall-clock is split three ways, exactly as the ranking/summary do it (see
``analysis/summary.py``): ``wall_clock_s = resource_wait_s + planning_startup_s +
active``. ``resource_wait_s`` is the pre-first-executor wait (clamped to overhead);
``active`` is the stage span (last completion − first submission);
``planning_startup_s`` is the remainder (admission/planning/driver/fetch). The
model's ``exec_time_s`` is the resource-wait-free job time ``active +
planning_startup_s`` (= ``wall_clock_s − resource_wait_s``): resource wait is a
queueing artifact of the on-prem cluster, not something the transport-aware model
should predict. For the same reason the group graph's ``scheduler_overhead_s`` is
the job's ``planning_startup_s`` only — never the resource wait.

This module also builds the per-run stage-dependency GROUP GRAPH the model's
job path (``model/predict_graph_groups.py``) consumes: the stage DAG (from each
stage's ``Parent IDs``) collapsed into wall-clock-concurrency groups (stages whose
run intervals overlap share a group), plus ``scheduler_overhead_s``. See
:func:`build_graph`.

No I/O here — this module accepts pre-parsed JSON dicts and returns dataclasses.
The pipeline module drives it over the raw zips.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

# Reuse the proven, pure helpers from the existing parser WITHOUT modifying it.
from .event_parser import (
    _FILE_READ_NAMES,
    _INTERNAL_METRICS,
    _PARTITION_READ_NAMES,
    _parse_memory_gb,
)

# The scan metric names we attribute per stage, mapped to their output field / kind.
_STATIC_FILE_READ_NAMES = {"static number of files read"}


def _metric_kind(name: str) -> Optional[str]:
    n = name.strip().lower()
    if n in _FILE_READ_NAMES:
        return "files"
    if n in _STATIC_FILE_READ_NAMES:
        return "static_files"
    if n in _PARTITION_READ_NAMES:
        return "partitions"
    return None


#: The transport constants the stage model cannot run without.
_TRANSPORT = ("window_max_bytes", "mtu", "network_latency_ms", "tsg_max_bw_gbs")

#: The campaign constants the published dataset carries. Not needed to score a
#: single run, but a run without them cannot be matched to its baseline or pooled
#: with the published rows, because the storage and cluster configuration is what
#: the matching keys on.
_CAMPAIGN = ("data_size_gb", "file_format", "compression_codec", "num_workers",
             "worker_disk_bw_gbps", "worker_net_bw_gbps", "tsg_avg_bw_gbs",
             "tsg_transfer_gb")

#: Disk geometry, recorded alongside ``worker_disk_bw_gbps`` rather than folded
#: into it. A worker's disk bandwidth has two independent ceilings - what its
#: volumes can deliver together, and what the instance's storage link allows -
#: and which one binds depends on how many volumes there are. One 250 MB/s
#: volume is limited by the volume; four of them are limited by the instance.
#: ``worker_disk_bw_gbps`` keeps the meaning the published dataset gave it, and
#: these say what it is made of, so nobody downstream has to reverse it.
_DISK_GEOMETRY = ("worker_disk_count", "worker_disk_bw_per_volume_gbps")

_FILE_FORMATS = ("parquet", "orc")
_CODECS = ("uncompressed", "snappy", "gzip")


@dataclass(frozen=True)
class EnvConstants:
    """Deployment and campaign constants the model needs but no event log contains.

    Each field is ``None`` when the operator did not supply it, in which case the
    matching output column is ``None`` too. Nothing here is ever guessed: an unset
    constant stays null rather than taking a plausible default, because a wrong
    ``tsg_max_bw_gbs`` or ``data_size_gb`` is an independent variable of the
    experiment and a silently wrong one produces a well-formed, fabricated dataset.

    The first four are the transport constants of the interconnect. The rest
    describe the storage configuration and the cluster, and exist because the
    published dataset carries them: they are the keys a network-constrained run is
    matched to its on-premises baseline on.

    ``worker_avg_cpu`` is deliberately absent. It is a node-level measurement, not a
    constant, and no Spark event log observes it; it needs a sampler on the workers.
    """
    window_max_bytes: Optional[int] = None
    mtu: Optional[int] = None
    network_latency_ms: Optional[float] = None
    tsg_max_bw_gbs: Optional[float] = None
    # Storage configuration of the dataset under test.
    data_size_gb: Optional[float] = None
    file_format: Optional[str] = None          # "parquet" | "orc"
    compression_codec: Optional[str] = None    # "uncompressed" | "snappy" | "gzip"
    # Cluster shape.
    num_workers: Optional[int] = None
    worker_disk_bw_gbps: Optional[float] = None
    worker_net_bw_gbps: Optional[float] = None
    # What the disk figure is made of. Optional, and additive: they explain
    # worker_disk_bw_gbps rather than replacing it, because that column is a
    # published one and redefining it would silently change what every existing
    # row means. gp3 throughput is derived from volume size by the provisioner,
    # so the per-volume rate is a provisioning fact worth carrying explicitly.
    worker_disk_count: Optional[int] = None
    worker_disk_bw_per_volume_gbps: Optional[float] = None
    # Observed interconnect use. Both are 0.0 on an on-premises baseline (no traffic
    # crosses the gateway); on a shaped cell they come from the gateway's counters.
    tsg_avg_bw_gbs: Optional[float] = None
    tsg_transfer_gb: Optional[float] = None

    def __post_init__(self) -> None:
        for name, allowed in (("file_format", _FILE_FORMATS),
                              ("compression_codec", _CODECS)):
            value = getattr(self, name)
            if value is not None and value not in allowed:
                raise ValueError(
                    f"{name}={value!r} is not one of {', '.join(allowed)}")
        if self.worker_disk_count is not None and self.worker_disk_count < 1:
            raise ValueError(
                f"worker_disk_count={self.worker_disk_count} must be at least 1")
        if (self.worker_disk_bw_per_volume_gbps is not None
                and self.worker_disk_bw_per_volume_gbps <= 0):
            raise ValueError("worker_disk_bw_per_volume_gbps must be positive")

    def missing(self) -> list[str]:
        """Transport constants that were not supplied (for a one-line notice)."""
        return [n for n in _TRANSPORT if getattr(self, n) is None]

    def missing_campaign(self) -> list[str]:
        """Campaign constants that were not supplied."""
        return [n for n in _CAMPAIGN if getattr(self, n) is None]

    def volume_disk_bw_gbps(self) -> Optional[float]:
        """What the worker's volumes can deliver together, before any other limit.

        Returned separately from ``worker_disk_bw_gbps`` on purpose. If this is the
        larger of the two, the instance's storage link is the binding constraint and
        the volumes are not the thing to change; if it is the smaller, they are.
        Neither conclusion is available from the published column alone.
        """
        if self.worker_disk_count is None or self.worker_disk_bw_per_volume_gbps is None:
            return None
        return round(self.worker_disk_count * self.worker_disk_bw_per_volume_gbps, 6)

    def total_worker_disk_bw_gbps(self) -> Optional[float]:
        if self.num_workers is None or self.worker_disk_bw_gbps is None:
            return None
        return round(self.num_workers * self.worker_disk_bw_gbps, 6)

    def total_worker_net_bw_gbps(self) -> Optional[float]:
        if self.num_workers is None or self.worker_net_bw_gbps is None:
            return None
        return round(self.num_workers * self.worker_net_bw_gbps, 6)

    def one_hot(self) -> dict[str, Optional[int]]:
        """The storage flags in the published dataset's one-hot encoding.

        All flags of a group are ``None`` when that group's value was not supplied,
        so an unset configuration reads as unknown rather than as "not parquet".
        """
        out: dict[str, Optional[int]] = {}
        for prefix, allowed, value in (("file_format", _FILE_FORMATS, self.file_format),
                                       ("compression_codec", _CODECS, self.compression_codec)):
            for option in allowed:
                out[f"{prefix}_{option}"] = None if value is None else int(value == option)
        return out


# The model reads these per stage (see stage_model.predict_stage_duration).
@dataclass
class StageFeature:
    query_id: str
    app_id: str
    app_name: str
    stage_id: int
    unique_stage_id: int
    total_stages: int
    # The TASK SPAN: last task finish minus the earliest task start, each task's
    # start reconstructed as finish - (executor_runtime + deserialize). This is
    # how the published dataset's column of the same name was measured
    # (parse_queries.py:200-207), and the two must mean the same thing or no
    # comparison against it holds.
    #
    # It is NOT submission -> completion. That reading also counts the stage
    # waiting for a task slot held by one of its own siblings, which the network
    # does not change and which is not additive across stages: measured on the
    # SF=1000 baseline, query4's stages carry 724 s of such waiting inside a
    # query that ran for 169 s, because its stages overlap about 5.6x.
    #
    # 0.0 when the stage reported no tasks - there is no task time to span.
    stage_duration_s: float
    num_tasks: int
    avg_task_duration_s: float
    executor_runtime_s: float
    executor_cpu_time_s: float
    input_read_gb: float
    # Shuffle volumes for this stage. Present in the listener's own stage records but
    # not in the released raw dataset, and needed to tell two structurally similar
    # stages apart when neither reads external input - see analysis/stage_matcher.py.
    shuffle_read_gb: float
    shuffle_write_gb: float
    number_of_files_read: Optional[int]
    static_number_of_files_read: Optional[int]
    number_of_partitions_read: Optional[int]
    spark_num_executors: int
    spark_executor_cores: int
    spark_executor_memory_gb: float
    # Deployment constants (None unless supplied). Kept last so the derived, always-real
    # columns read first.
    window_max_bytes: Optional[int]
    mtu: Optional[int]
    network_latency_ms: Optional[float]
    tsg_max_bw_gbs: Optional[float]
    # Storage configuration and cluster shape, from EnvConstants. Null unless
    # supplied. These are what the published dataset matches a run to its baseline on.
    data_size_gb: Optional[float] = None
    file_format_parquet: Optional[int] = None
    file_format_orc: Optional[int] = None
    compression_codec_uncompressed: Optional[int] = None
    compression_codec_snappy: Optional[int] = None
    compression_codec_gzip: Optional[int] = None
    num_workers: Optional[int] = None
    worker_disk_bw_gbps: Optional[float] = None
    worker_net_bw_gbps: Optional[float] = None
    total_worker_disk_bw_gbps: Optional[float] = None
    total_worker_net_bw_gbps: Optional[float] = None
    worker_disk_count: Optional[int] = None
    worker_disk_bw_per_volume_gbps: Optional[float] = None
    worker_volume_disk_bw_gbps: Optional[float] = None
    # Always null: node CPU is not in any Spark event log. Kept so the schema lines up
    # with the published dataset instead of the column vanishing without explanation.
    worker_avg_cpu: Optional[float] = None
    # Which slice of the application this row belongs to. "application" (the whole
    # run, the production default) or "sql_execution" (one query of a multi-query
    # benchmark). ``unit_id`` is the app id or the SQL execution id. See
    # ``run_splitter``.
    unit_kind: str = "application"
    unit_id: str = ""


@dataclass
class JobFeature:
    query_id: str
    app_id: str
    app_name: str
    # The model's observed job time (predict_graph_groups scores against this): the
    # resource-wait-free wall-clock = active + planning_startup_s = wall_clock_s −
    # resource_wait_s. Resource wait is excluded so it matches a prediction whose
    # scheduler_overhead_s is planning/startup only.
    exec_time_s: float
    # The full wall-clock split (wall = resource_wait + planning_startup + active).
    wall_clock_s: float
    resource_wait_s: float
    planning_startup_s: float
    num_spark_stages: int
    spark_total_executor_runtime_s: float
    spark_total_executor_cpu_time_s: float
    spark_total_input_read_gb: float
    spark_total_shuffle_read_gb: float
    spark_num_executors: int
    spark_executor_cores: int
    spark_executor_memory_gb: float
    network_latency_ms: Optional[float]
    tsg_max_bw_gbs: Optional[float]
    mtu: Optional[int]
    window_max_bytes: Optional[int]
    # The unclamped stage span (last completion - first submission). Equal to
    # ``active`` for an application unit. For a query unit it can exceed the SQL
    # span, because a stage submitted while the previous query was still finishing
    # already carries this query's execution id; ``active`` is clamped so the
    # wall-clock parts still sum, and this field keeps the raw value visible.
    stage_span_s: float = 0.0
    # An ALTERNATIVE job time, not a correction of ``exec_time_s``: what the benchmark
    # harness's own timer recorded for this query, carried in through the run's side-car
    # because no event log holds it. It brackets more than the SQL execution - parse,
    # analysis and optimization, plus any job the driver ran before posting
    # SQLExecutionStart - so it is consistently the larger of the two. Emitted because
    # this, not ``exec_time_s``, is how the published dataset's ``exec_time_s`` was
    # measured. ``None`` when no side-car supplied it.
    harness_exec_time_s: Optional[float] = None
    # Storage configuration and observed interconnect use, from EnvConstants.
    data_size_gb: Optional[float] = None
    file_format_parquet: Optional[int] = None
    file_format_orc: Optional[int] = None
    compression_codec_uncompressed: Optional[int] = None
    compression_codec_snappy: Optional[int] = None
    compression_codec_gzip: Optional[int] = None
    tsg_avg_bw_gbs: Optional[float] = None
    tsg_transfer_gb: Optional[float] = None
    # Always null; see StageFeature.worker_avg_cpu.
    worker_avg_cpu: Optional[float] = None
    unit_kind: str = "application"
    unit_id: str = ""
    # Absolute wall-clock bounds of the unit, epoch milliseconds, straight from the
    # event log. Durations alone cannot be joined to anything measured outside
    # Spark - a gateway's byte counters, a node CPU sampler - because attributing
    # an external time series to a query needs to know WHEN the query ran, not
    # only how long it took. Null when the log carried no usable bounds.
    start_ts_ms: Optional[int] = None
    end_ts_ms: Optional[int] = None


@dataclass
class GraphFeature:
    """The per-run stage-dependency group graphs the model's job path consumes.

    ``query_id``/``app_id`` match the job and stage records so the three files join.
    ``scheduler_overhead_s`` is the job's planning/startup time (resource wait
    excluded), added once at the job level by the consumer.

    Two group graphs are built over the same stages, and they differ only in how the
    groups are formed. Keep both: they answer different questions.

    ``dag_groups`` is the published model's rule (IEEE Access, Sec. IV-E): stages that
    share a dependency level in ``stage_edges`` form one group. It needs no timing, so
    it is computable at submission time and it is what a predictor may use.

    ``observed_groups`` is built from wall-clock overlap: stages whose
    ``[submission, completion]`` intervals overlap share a group, and each group is
    then split into antichains so no two members are DAG-comparable. It reads the
    observed timeline, so it is an oracle. Use it as a ceiling, never as a predictor.

    ``groups``/``group_edges``/``stage_to_group`` remain as an alias of the
    ``observed_*`` set so existing consumers keep working.
    """
    query_id: str
    app_id: str
    name: str
    source: str
    scheduler_overhead_s: float
    stages: list[int]
    stage_edges: list[list[int]]
    dag_groups: list[dict[str, Any]]
    dag_group_edges: list[list[int]]
    dag_stage_to_group: dict[int, int]
    observed_groups: list[dict[str, Any]]
    observed_group_edges: list[list[int]]
    observed_stage_to_group: dict[int, int]
    groups: list[dict[str, Any]]
    group_edges: list[list[int]]
    stage_to_group: dict[int, int]
    unit_kind: str = "application"
    unit_id: str = ""


def _bytes_to_gb(b: float) -> float:
    """Decimal GB (÷1e9), matching the transport-aware pipeline's unit convention."""
    return b / 1e9


class _State:
    def __init__(self) -> None:
        self.app_id: str = ""
        self.app_name: str = ""
        self.start_ts: int = 0
        self.end_ts: int = 0

        self.executor_cores: int = 0
        self.executor_memory: str = ""
        self._live_executors: set[str] = set()
        self._peak_executors: int = 0
        # Timestamp of the first non-driver ExecutorAdded. [start, first executor] is
        # pure resource-acquisition wait; becomes resource_wait_s (clamped to overhead).
        self._first_executor_ts: int = 0

        # stage_id -> per-stage aggregates from SparkListenerStageCompleted.
        self.stages: dict[int, dict[str, Any]] = {}
        # Earliest task Launch Time per stage (SparkListenerTaskStart). Its gap to the
        # stage Submission Time is the executor-acquisition wait inside the stage.
        self._stage_first_task_ts: dict[int, int] = {}
        # stage_id -> [earliest reconstructed task start, latest task finish], in ms.
        # Feeds ``stage_duration_s``; see the note on that field.
        self._stage_task_span: dict[int, list[int]] = {}

        # --- SQL scan-metric attribution (per-stage files/partitions) ---
        # stage_id -> SQL execution id (from StageSubmitted properties).
        self.stageid_to_execid: dict[int, int] = {}
        # exec_id -> SQLExecutionStart / SQLExecutionEnd timestamps. These bound a
        # query inside a multi-query application; see ``run_splitter``.
        self.sql_exec_start: dict[int, int] = {}
        self.sql_exec_end: dict[int, int] = {}
        # exec_id -> {acc_id -> "files"|"static_files"|"partitions"} from the plan.
        self.acc_kind: dict[int, dict[int, str]] = defaultdict(dict)
        # exec_id -> {acc_id -> plan node name} (fallback key into RDD-scope map).
        self.acc_node: dict[int, dict[int, str]] = defaultdict(dict)
        # exec_id -> {acc_id -> value}. Driver updates are authoritative; per-task
        # updates accumulate for the versions that push these metrics per task.
        self.accum_value: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        # exec_id -> {acc_id -> {stage_id}} (primary mapping, from tasks / adaptive).
        self.accid_to_stageids: dict[int, dict[int, set[int]]] = defaultdict(lambda: defaultdict(set))
        # exec_id -> {node name -> {stage_id}} (fallback mapping, from RDD Info Scope).
        self.node_name_to_stageids: dict[int, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))


def _walk_plan(node: Any, exec_id: int, state: _State) -> None:
    """Record file/partition-read accumulator ids and their node names, per execution."""
    if not isinstance(node, dict):
        return
    node_name = node.get("nodeName", "?")
    for metric in node.get("metrics", []):
        try:
            acc_id = int(metric["accumulatorId"])
        except (KeyError, ValueError, TypeError):
            continue
        kind = _metric_kind(metric.get("name", ""))
        if kind:
            state.acc_kind[exec_id][acc_id] = kind
            state.acc_node[exec_id][acc_id] = node_name
    for child in node.get("children", []):
        _walk_plan(child, exec_id, state)


def _handle_application_start(event: dict, state: _State) -> None:
    state.app_id = event.get("App ID", "")
    state.app_name = event.get("App Name", "")
    state.start_ts = event.get("Timestamp", 0)


def _handle_application_end(event: dict, state: _State) -> None:
    state.end_ts = event.get("Timestamp", 0)


def _handle_environment_update(event: dict, state: _State) -> None:
    raw = event.get("Spark Properties", {})
    if isinstance(raw, list):
        props = {p[0]: p[1] for p in raw if isinstance(p, (list, tuple)) and len(p) == 2}
    elif isinstance(raw, dict):
        props = raw
    else:
        return
    state.executor_memory = props.get("spark.executor.memory", "") or state.executor_memory


def _handle_resource_profile_added(event: dict, state: _State) -> None:
    try:
        state.executor_cores = int(event["Executor Resource Requests"]["cores"]["Amount"])
    except (KeyError, ValueError, TypeError):
        pass


def _handle_executor_added(event: dict, state: _State) -> None:
    exec_id = event.get("Executor ID", "")
    if exec_id and exec_id != "driver":
        state._live_executors.add(exec_id)
        state._peak_executors = max(state._peak_executors, len(state._live_executors))
        ts = event.get("Timestamp", 0)
        if state._first_executor_ts == 0 and ts:
            state._first_executor_ts = ts
    if state.executor_cores == 0:
        try:
            state.executor_cores = int(event["Executor Info"]["Total Cores"])
        except (KeyError, ValueError, TypeError):
            pass


def _handle_executor_removed(event: dict, state: _State) -> None:
    state._live_executors.discard(event.get("Executor ID", ""))


def _handle_task_start(event: dict, state: _State) -> None:
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


def _handle_stage_submitted(event: dict, state: _State) -> None:
    """Link a stage to its SQL execution id (needed to scope the scan-metric maps)."""
    try:
        stage_id = int(event["Stage Info"]["Stage ID"])
        exec_id = int(event.get("Properties", {}).get("spark.sql.execution.id"))
    except (KeyError, ValueError, TypeError):
        return
    state.stageid_to_execid[stage_id] = exec_id


def _handle_stage_completed(event: dict, state: _State) -> None:
    try:
        info = event["Stage Info"]
        stage_id = int(info["Stage ID"])
    except (KeyError, ValueError, TypeError):
        return

    num_tasks = int(info.get("Number of Tasks", 0))
    submission_ts = info.get("Submission Time", 0) or 0
    completion_ts = info.get("Completion Time", 0) or 0
    duration_ms = max(0, completion_ts - submission_ts) if submission_ts and completion_ts else 0

    # The stage DAG: this stage depends on its Parent IDs. Kept for the group graph.
    parent_ids: list[int] = []
    for p in info.get("Parent IDs", []):
        try:
            parent_ids.append(int(p))
        except (ValueError, TypeError):
            pass

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
        mapped = _INTERNAL_METRICS.get(acc.get("Name", ""))
        if mapped:
            try:
                metrics[mapped] = float(acc["Value"])
            except (KeyError, ValueError, TypeError):
                pass

    # A stage attempt can complete more than once (retries); keep the latest.
    state.stages[stage_id] = {
        "stage_id": stage_id,
        "num_tasks": num_tasks,
        "duration_ms": duration_ms,
        "submission_ts": submission_ts,
        "completion_ts": completion_ts,
        "parent_ids": parent_ids,
        **metrics,
    }

    # Fallback attribution key: map each scan node's name to this stage via RDD scope.
    exec_id = state.stageid_to_execid.get(stage_id)
    if exec_id is not None:
        for rdd in info.get("RDD Info", []):
            scope_str = rdd.get("Scope", "")
            if not isinstance(scope_str, str) or not scope_str.startswith("{"):
                continue
            try:
                name = json.loads(scope_str).get("name")
            except (ValueError, TypeError):
                continue
            if name:
                state.node_name_to_stageids[exec_id][name].add(stage_id)


def _handle_task_end(event: dict, state: _State) -> None:
    """Accumulate the stage's task span, then do the scan-accumulator attribution.

    The span is the published dataset's stage duration: a task's start is
    *reconstructed* as ``Finish Time - (Executor Run Time + Executor Deserialize
    Time)`` rather than read from its launch, so scheduler delay and result fetch fall
    outside it. See ``StageFeature.stage_duration_s``.
    """
    try:
        stage_id = int(event["Stage ID"])
    except (KeyError, ValueError, TypeError):
        return

    finish = (event.get("Task Info") or {}).get("Finish Time")
    if isinstance(finish, int) and finish > 0:
        metrics = event.get("Task Metrics") or {}
        busy_ms = (metrics.get("Executor Run Time", 0)
                   + metrics.get("Executor Deserialize Time", 0))
        start = finish - busy_ms
        span = state._stage_task_span.get(stage_id)
        if span is None:
            state._stage_task_span[stage_id] = [start, finish]
        else:
            if start < span[0]:
                span[0] = start
            if finish > span[1]:
                span[1] = finish

    exec_id = state.stageid_to_execid.get(stage_id)
    if exec_id is None or not state.acc_kind.get(exec_id):
        return
    tracked = state.acc_kind[exec_id]
    for acc in event.get("Task Info", {}).get("Accumulables", []):
        try:
            acc_id = int(acc["ID"])
        except (KeyError, ValueError, TypeError):
            continue
        if acc_id not in tracked:
            continue
        state.accid_to_stageids[exec_id][acc_id].add(stage_id)
        try:
            state.accum_value[exec_id][acc_id] += int(acc.get("Update", 0))
        except (ValueError, TypeError):
            pass


def _handle_driver_accum_updates(event: dict, state: _State) -> None:
    """Authoritative per-query values for the scan metrics (driver-side)."""
    try:
        exec_id = int(event["executionId"])
    except (KeyError, ValueError, TypeError):
        return
    tracked = state.acc_kind.get(exec_id, {})
    for update in event.get("accumUpdates", []):
        if not (isinstance(update, (list, tuple)) and len(update) >= 2):
            continue
        try:
            acc_id = int(update[0])
        except (ValueError, TypeError):
            continue
        if acc_id in tracked:
            try:
                state.accum_value[exec_id][acc_id] = int(update[1])
            except (ValueError, TypeError):
                pass


def _handle_sql_start(event: dict, state: _State) -> None:
    try:
        exec_id = int(event["executionId"])
    except (KeyError, ValueError, TypeError):
        return
    ts = event.get("time")
    if isinstance(ts, int) and ts > 0 and exec_id not in state.sql_exec_start:
        state.sql_exec_start[exec_id] = ts
    plan = event.get("sparkPlanInfo")
    if plan:
        _walk_plan(plan, exec_id, state)


def _handle_sql_end(event: dict, state: _State) -> None:
    """Close a SQL execution's wall-clock span (the query unit's end)."""
    try:
        exec_id = int(event["executionId"])
    except (KeyError, ValueError, TypeError):
        return
    ts = event.get("time")
    if isinstance(ts, int) and ts > 0:
        state.sql_exec_end[exec_id] = max(ts, state.sql_exec_end.get(exec_id, 0))


def _handle_sql_adaptive_update(event: dict, state: _State) -> None:
    # An adaptive re-plan re-reports the plan, never a new start time.
    exec_id = event.get("executionId")
    plan = event.get("sparkPlanInfo")
    if plan is None:
        return
    try:
        _walk_plan(plan, int(exec_id), state)
    except (ValueError, TypeError):
        return


def _handle_sql_adaptive_metric_updates(event: dict, state: _State) -> None:
    """Adaptive executions can carry both values and explicit stage ids per metric."""
    try:
        exec_id = int(event["executionId"])
    except (KeyError, ValueError, TypeError):
        return
    tracked = state.acc_kind.get(exec_id, {})
    for upd in event.get("accumUpdates", []) or []:
        if isinstance(upd, (list, tuple)) and len(upd) >= 2:
            try:
                acc_id = int(upd[0])
            except (ValueError, TypeError):
                continue
            if acc_id in tracked:
                try:
                    state.accum_value[exec_id][acc_id] = int(upd[1])
                except (ValueError, TypeError):
                    pass
        elif isinstance(upd, dict):
            try:
                acc_id = int(upd.get("accumulatorId"))
            except (ValueError, TypeError):
                continue
            if acc_id not in tracked:
                continue
            if "value" in upd:
                try:
                    state.accum_value[exec_id][acc_id] = int(upd["value"])
                except (ValueError, TypeError):
                    pass
            for sid in upd.get("stageIds", []):
                try:
                    state.accid_to_stageids[exec_id][acc_id].add(int(sid))
                except (ValueError, TypeError):
                    pass


_EVENT_HANDLERS = {
    "SparkListenerApplicationStart": _handle_application_start,
    "SparkListenerApplicationEnd": _handle_application_end,
    "SparkListenerEnvironmentUpdate": _handle_environment_update,
    "SparkListenerResourceProfileAdded": _handle_resource_profile_added,
    "SparkListenerExecutorAdded": _handle_executor_added,
    "SparkListenerExecutorRemoved": _handle_executor_removed,
    "SparkListenerTaskStart": _handle_task_start,
    "SparkListenerStageSubmitted": _handle_stage_submitted,
    "SparkListenerStageCompleted": _handle_stage_completed,
    "SparkListenerTaskEnd": _handle_task_end,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart": _handle_sql_start,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd": _handle_sql_end,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate": _handle_sql_adaptive_update,
    "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates": _handle_driver_accum_updates,
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveSQLMetricUpdates": _handle_sql_adaptive_metric_updates,
}


def process_event(event: dict, state: _State) -> None:
    handler = _EVENT_HANDLERS.get(event.get("Event", ""))
    if handler is not None:
        handler(event, state)


def _is_scan_stage(stage: dict[str, Any]) -> bool:
    """Reads external input and does no shuffle read — a pure scan. Its task count is
    the RDD-side proxy for the splits/files touched."""
    return stage["input_bytes"] > 0 and (
        stage["shuffle_read_remote_bytes"] + stage["shuffle_read_local_bytes"]
    ) == 0


def _resolve_scan_counts(state: _State) -> dict[int, dict[str, int]]:
    """Attribute each SQL scan metric value to its stage(s).

    Value comes from ``accum_value`` (driver-authoritative); stage(s) from the
    primary accumulator->stage map, falling back to the node-name->stage map. When a
    metric maps to several stages the full value is credited to each: Spark reports
    one total and there is no per-stage split.

    **One accumulator per (metric, plan node), and assigned rather than summed.**
    Adaptive re-planning issues a FRESH accumulator id for the same logical scan
    every time it re-plans, and they all resolve to the same node name and so to
    the same stages. Summing them adds one scan to itself once per plan version:
    measured on 7.3.1's own event log, `store_sales` in execution 6 carries
    accumulators 10029 (30) and 13534 (1094) on stages [133, 143, 162], and
    summing gave 1124 where the published parser reports 1094. Stage 162 sits in
    two nodes' sets and got 3372 against a published 30.

    Verified against ground truth rather than argued: re-parsing 7.3.1's own log
    and joining on unique_stage_id, summing gives 388,529 files against the
    published 162,980 (2.38x, 873 of 1105 stages exact); assigning per
    (metric, node) gives 164,682 (1.010x, 1098 of 1105 exact). It is what
    spark-exec-predictor's file_access.py does too - `entry[field] = int(value)`,
    not `+=`.

    Returns ``{stage_id: {"files"|"static_files"|"partitions": int}}``.
    """
    out: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for exec_id, accmap in state.acc_kind.items():
        # (metric kind, plan node) -> the last accumulator seen for it, with the
        # stages it resolves to. Later plan versions overwrite earlier ones.
        chosen: dict[tuple[str, Any], tuple[int, set[int]]] = {}
        for acc_id, kind in accmap.items():
            value = state.accum_value[exec_id].get(acc_id, 0)
            if value <= 0:
                continue
            node = state.acc_node[exec_id].get(acc_id)
            stages = state.accid_to_stageids[exec_id].get(acc_id)
            if not stages:
                stages = state.node_name_to_stageids[exec_id].get(node) if node else None
            if not stages:
                continue
            chosen[(kind, node)] = (value, stages)
        for (kind, _node), (value, stages) in chosen.items():
            for sid in stages:
                out[sid][kind] = value
    return out


def _stage_task_span_s(state: _State, stage_id: int) -> Optional[float]:
    """The stage's task span in seconds, or ``None`` when it reported no tasks."""
    span = state._stage_task_span.get(stage_id)
    if not span or span[1] < span[0]:
        return None
    return round((span[1] - span[0]) / 1000.0, 6)


def _time_breakdown(state: _State) -> dict[str, float]:
    """Split the job wall-clock the same way analysis/summary.py does, exactly.

    ``wall_clock_s = resource_wait_s + planning_startup_s + active`` where
    ``active`` = last stage completion − first stage submission. ``exec_time_s`` is the
    resource-wait-free job time (``active + planning_startup_s``); it is what the model
    scores its prediction against, so resource wait is kept out of it.
    """
    wall_s = (state.end_ts - state.start_ts) / 1000.0 if state.end_ts > state.start_ts else 0.0

    submissions = [s["submission_ts"] for s in state.stages.values() if s["submission_ts"]]
    completions = [s["completion_ts"] for s in state.stages.values() if s["completion_ts"]]
    stage_span_s = (max(completions) - min(submissions)) / 1000.0 if submissions and completions else 0.0
    stage_span_s = max(0.0, stage_span_s)
    # For an application the stages always sit inside [start, end], so this clamp is a
    # no-op. For a query unit the span can straddle the SQL boundary (a stage carrying
    # this execution id was submitted while the previous query was still finishing), and
    # without the clamp the three wall-clock parts would not sum. The raw span is kept.
    active_s = min(stage_span_s, wall_s) if wall_s > 0 else stage_span_s

    overhead_s = max(0.0, wall_s - active_s)
    if state._first_executor_ts and state.start_ts and state._first_executor_ts > state.start_ts:
        resource_wait_s = min((state._first_executor_ts - state.start_ts) / 1000.0, overhead_s)
    else:
        resource_wait_s = 0.0
    planning_s = max(0.0, wall_s - active_s - resource_wait_s)

    return {
        "wall_clock_s": round(wall_s, 3),
        "active_exec_time_s": round(active_s, 3),
        "stage_span_s": round(stage_span_s, 3),
        "resource_wait_s": round(resource_wait_s, 3),
        "planning_startup_s": round(planning_s, 3),
        # active + planning_startup, computed from the rounded parts so the fields agree.
        "exec_time_s": round(max(0.0, wall_s - resource_wait_s), 3),
    }


def build_results(
    state: _State, env: EnvConstants | None = None, unit: Any = None
) -> tuple[Optional[JobFeature], list[StageFeature]]:
    """Assemble the per-run job feature and its per-stage features.

    Returns ``(None, [])`` when the log carried no ApplicationStart (nothing to key on).

    ``unit`` is an optional :class:`run_splitter.Unit` naming the slice of the
    application these stages came from. Without it the run is the whole application
    and ``query_id`` is the application id, exactly as before.
    """
    if not state.app_id:
        return None, []
    env = env or EnvConstants()
    query_id = unit.query_id if unit is not None else state.app_id
    unit_kind = unit.unit_kind if unit is not None else "application"
    unit_id = unit.unit_id if unit is not None else state.app_id
    harness_exec_time_s = getattr(unit, "harness_exec_time_s", None)

    num_executors = state._peak_executors
    memory_gb = _parse_memory_gb(state.executor_memory)
    total_stages = len(state.stages)
    times = _time_breakdown(state)
    scan_counts = _resolve_scan_counts(state)

    one_hot = env.one_hot()
    stage_features: list[StageFeature] = []
    total_runtime = total_cpu = total_input = total_shuffle_read = 0.0

    for s in state.stages.values():
        runtime_s = s["executor_runtime_ms"] / 1000.0
        cpu_s = s["executor_cpu_time_ns"] / 1e9
        input_gb = _bytes_to_gb(s["input_bytes"])
        shuffle_read_gb = _bytes_to_gb(
            s["shuffle_read_remote_bytes"] + s["shuffle_read_local_bytes"]
        )
        num_tasks = s["num_tasks"]
        avg_task_s = runtime_s / num_tasks if num_tasks > 0 else 0.0

        counts = scan_counts.get(s["stage_id"], {})
        files = counts.get("files")
        static_files = counts.get("static_files")
        partitions = counts.get("partitions")
        # Backfill one file count from the other (planned vs resolved), as file_access.py does.
        if files is None and static_files is not None:
            files = static_files
        if static_files is None and files is not None:
            static_files = files
        # Last resort for files: the scan-task-count proxy on a pure-scan stage.
        if files is None and _is_scan_stage(s):
            files = num_tasks

        # Partitions falls back to files, because the published column does.
        # spark-exec-predictor's parse_queries.py:239-243:
        #
        #     number_of_partitions_read = file_access_metrics_for_stage.get(
        #         "number_of_partitions_read", number_of_files_read)
        #     if number_of_partitions_read == None:
        #         number_of_partitions_read = number_of_files_read
        #
        # and it fires nearly always: in the published dataset
        # number_of_partitions_read equals number_of_files_read on 5285 of 5401
        # on-prem rows (97.9%), differing only where a real partition metric
        # existed. So that column is mostly a file count wearing a partition
        # name, and reproducing it means reproducing the fallback.
        #
        # Without this the two campaigns disagree on WHICH stages carry the
        # metric rather than on its value: 606 of 606 input-reading stages in
        # 7.3.1 against 230 of 612 here. The model divides it by task count into
        # `partition_access_per_task` and multiplies by the wave count, so
        # concentrating it on the big multi-wave stages inflated the sum of
        # rho*waves 2.25x, and beta_storage fitted 2.25x low to absorb it.
        if partitions is None:
            partitions = files

        total_runtime += runtime_s
        total_cpu += cpu_s
        total_input += input_gb
        total_shuffle_read += shuffle_read_gb

        stage_features.append(StageFeature(
            query_id=query_id,
            app_id=state.app_id,
            app_name=state.app_name,
            stage_id=s["stage_id"],
            unique_stage_id=s["stage_id"],
            total_stages=total_stages,
            stage_duration_s=_stage_task_span_s(state, s["stage_id"]) or 0.0,
            num_tasks=num_tasks,
            avg_task_duration_s=round(avg_task_s, 6),
            executor_runtime_s=round(runtime_s, 3),
            executor_cpu_time_s=round(cpu_s, 3),
            input_read_gb=round(input_gb, 9),
            shuffle_read_gb=round(shuffle_read_gb, 9),
            shuffle_write_gb=round(_bytes_to_gb(s["shuffle_write_bytes"]), 9),
            number_of_files_read=files,
            static_number_of_files_read=static_files,
            number_of_partitions_read=partitions,
            spark_num_executors=num_executors,
            spark_executor_cores=state.executor_cores,
            spark_executor_memory_gb=memory_gb,
            window_max_bytes=env.window_max_bytes,
            mtu=env.mtu,
            network_latency_ms=env.network_latency_ms,
            tsg_max_bw_gbs=env.tsg_max_bw_gbs,
            data_size_gb=env.data_size_gb,
            num_workers=env.num_workers,
            worker_disk_bw_gbps=env.worker_disk_bw_gbps,
            worker_net_bw_gbps=env.worker_net_bw_gbps,
            total_worker_disk_bw_gbps=env.total_worker_disk_bw_gbps(),
            total_worker_net_bw_gbps=env.total_worker_net_bw_gbps(),
            worker_disk_count=env.worker_disk_count,
            worker_disk_bw_per_volume_gbps=env.worker_disk_bw_per_volume_gbps,
            worker_volume_disk_bw_gbps=env.volume_disk_bw_gbps(),
            unit_kind=unit_kind,
            unit_id=unit_id,
            **one_hot,
        ))

    stage_features.sort(key=lambda s: s.stage_id)

    job = JobFeature(
        query_id=query_id,
        app_id=state.app_id,
        app_name=state.app_name,
        exec_time_s=times["exec_time_s"],
        wall_clock_s=times["wall_clock_s"],
        resource_wait_s=times["resource_wait_s"],
        planning_startup_s=times["planning_startup_s"],
        num_spark_stages=total_stages,
        spark_total_executor_runtime_s=round(total_runtime, 3),
        spark_total_executor_cpu_time_s=round(total_cpu, 3),
        spark_total_input_read_gb=round(total_input, 9),
        spark_total_shuffle_read_gb=round(total_shuffle_read, 9),
        spark_num_executors=num_executors,
        spark_executor_cores=state.executor_cores,
        spark_executor_memory_gb=memory_gb,
        network_latency_ms=env.network_latency_ms,
        tsg_max_bw_gbs=env.tsg_max_bw_gbs,
        mtu=env.mtu,
        window_max_bytes=env.window_max_bytes,
        stage_span_s=times["stage_span_s"],
        harness_exec_time_s=harness_exec_time_s,
        data_size_gb=env.data_size_gb,
        tsg_avg_bw_gbs=env.tsg_avg_bw_gbs,
        tsg_transfer_gb=env.tsg_transfer_gb,
        unit_kind=unit_kind,
        unit_id=unit_id,
        start_ts_ms=(unit.start_ts if unit is not None else state.start_ts) or None,
        end_ts_ms=(unit.end_ts if unit is not None else state.end_ts) or None,
        **one_hot,
    )
    return job, stage_features


def _descendants(stage_ids: list[int], edges: list[tuple[int, int]]) -> dict[int, set[int]]:
    """Transitive descendants of each stage in the DAG (for the comparability test)."""
    children: dict[int, list[int]] = defaultdict(list)
    for u, v in edges:
        children[u].append(v)
    desc: dict[int, set[int]] = {}

    def walk(root: int) -> set[int]:
        seen: set[int] = set()
        stack = list(children.get(root, ()))
        while stack:
            n = stack.pop()
            if n not in seen:
                seen.add(n)
                stack.extend(children.get(n, ()))
        return seen

    for sid in stage_ids:
        desc[sid] = walk(sid)
    return desc


def _overlap_groups(
    stage_ids: list[int],
    intervals: dict[int, tuple[int, int]],
    desc: dict[int, set[int]],
) -> list[list[int]]:
    """Cluster stages into wall-clock-concurrency groups, keeping each an antichain.

    Two stages join the same cluster when their ``[submission, completion]`` intervals
    overlap (connected components). A cluster is then split so that no two members are
    DAG-comparable (one a transitive ancestor of the other): the model treats a group as
    overlapping/parallel work, so dependent stages must never share one — this is the
    ``no stage_edge inside a group`` invariant the model relies on. A stage with no timing
    stays its own singleton. Groups are returned sorted by their smallest member id, so
    the caller's group ids are deterministic.
    """
    parent = {sid: sid for sid in stage_ids}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    ids = sorted(stage_ids)
    for i, a in enumerate(ids):
        sa = intervals.get(a)
        if not sa:
            continue
        for b in ids[i + 1:]:
            sb = intervals.get(b)
            if not sb:
                continue
            # Half-open interval overlap: a starts before b ends AND b starts before a ends.
            if sa[0] < sb[1] and sb[0] < sa[1]:
                union(a, b)

    comps: dict[int, list[int]] = defaultdict(list)
    for sid in ids:
        comps[find(sid)].append(sid)

    def comparable(a: int, b: int) -> bool:
        return b in desc[a] or a in desc[b]

    groups: list[list[int]] = []
    for members in comps.values():
        # Greedy antichain colouring: place each stage in the first sub-group with no
        # member comparable to it. A component that is already an antichain stays whole.
        subgroups: list[list[int]] = []
        for sid in sorted(members):
            for sg in subgroups:
                if all(not comparable(sid, m) for m in sg):
                    sg.append(sid)
                    break
            else:
                subgroups.append([sid])
        groups.extend(subgroups)

    return sorted(groups, key=lambda m: m[0])


def _dag_level_groups(
    stage_ids: list[int], edges: list[tuple[int, int]]
) -> list[list[int]]:
    """Cluster stages by their dependency level in the DAG (the published rule).

    The level of a stage is its longest-path distance from a root, so every stage in
    one group is independent of the others and each group's parents all sit in an
    earlier group. Uses no timing, so it is computable before the run.

    A stage with no ``Parent IDs`` lands on level 0. Note that Spark's ``Parent IDs``
    records shuffle dependencies inside one Spark job only, so two actions in one
    application look independent here even when they ran strictly in sequence.
    Groups are returned sorted by their smallest member id, so group ids are
    deterministic.
    """
    children: dict[int, list[int]] = defaultdict(list)
    indeg: dict[int, int] = {sid: 0 for sid in stage_ids}
    for u, v in edges:
        children[u].append(v)
        indeg[v] += 1

    level: dict[int, int] = {sid: 0 for sid in stage_ids}
    queue = [sid for sid in sorted(stage_ids) if indeg[sid] == 0]
    while queue:
        n = queue.pop()
        for c in children.get(n, ()):
            if level[c] < level[n] + 1:
                level[c] = level[n] + 1
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)

    buckets: dict[int, list[int]] = defaultdict(list)
    for sid in sorted(stage_ids):
        buckets[level[sid]].append(sid)

    return sorted(buckets.values(), key=lambda m: m[0])


def _lift_groups(
    members_list: list[list[int]], stage_edges: list[tuple[int, int]]
) -> tuple[list[dict[str, Any]], list[list[int]], dict[int, int]]:
    """Turn a member partition into the group records, edges, and stage->group map.

    ``group_edges`` is ``stage_edges`` lifted through the map, with intra-group edges
    dropped and duplicates removed.
    """
    stage_to_group: dict[int, int] = {}
    for gid, members in enumerate(members_list):
        for sid in members:
            stage_to_group[sid] = gid

    ge: set[tuple[int, int]] = set()
    for u, v in stage_edges:
        gu, gv = stage_to_group[u], stage_to_group[v]
        if gu != gv:
            ge.add((gu, gv))
    group_edges = sorted(ge)

    preds: dict[int, set[int]] = defaultdict(set)
    for gu, gv in group_edges:
        preds[gv].add(gu)

    groups = [
        {
            "gid": gid,
            "members": members,
            "kind": "parallel" if len(members) > 1 else "sequential",
            "parents": sorted(preds[gid]),
        }
        for gid, members in enumerate(members_list)
    ]

    return groups, [[u, v] for u, v in group_edges], stage_to_group


def build_graph(state: _State, unit: Any = None) -> Optional[GraphFeature]:
    """Build the per-run stage-dependency group graph from the parsed event log.

    Returns ``None`` when the log carried no ApplicationStart. A run with no completed
    stages yields an empty graph (still keyed by ``query_id`` for a clean join).
    """
    if not state.app_id:
        return None
    times = _time_breakdown(state)
    query_id = unit.query_id if unit is not None else state.app_id
    unit_kind = unit.unit_kind if unit is not None else "application"
    unit_id = unit.unit_id if unit is not None else state.app_id

    stage_ids = sorted(state.stages)
    known = set(stage_ids)

    # stage_edges: parent -> child from each stage's Parent IDs (both endpoints known).
    edges: set[tuple[int, int]] = set()
    for sid in stage_ids:
        for p in state.stages[sid].get("parent_ids", []):
            if p in known and p != sid:
                edges.add((p, sid))
    stage_edges = sorted(edges)

    intervals: dict[int, tuple[int, int]] = {}
    for sid in stage_ids:
        sub = state.stages[sid]["submission_ts"]
        comp = state.stages[sid]["completion_ts"]
        if sub and comp and comp >= sub:
            intervals[sid] = (sub, comp)

    desc = _descendants(stage_ids, stage_edges)

    # Two groupings over the same stages. The DAG one uses no timing and is what a
    # predictor may use; the observed one reads the timeline and is a ceiling.
    dag_groups, dag_group_edges, dag_stage_to_group = _lift_groups(
        _dag_level_groups(stage_ids, stage_edges), stage_edges
    )
    obs_groups, obs_group_edges, obs_stage_to_group = _lift_groups(
        _overlap_groups(stage_ids, intervals, desc), stage_edges
    )

    return GraphFeature(
        query_id=query_id,
        app_id=state.app_id,
        name=state.app_name,
        source=state.app_id,
        scheduler_overhead_s=times["planning_startup_s"],
        stages=stage_ids,
        stage_edges=[[u, v] for u, v in stage_edges],
        dag_groups=dag_groups,
        dag_group_edges=dag_group_edges,
        dag_stage_to_group=dag_stage_to_group,
        observed_groups=obs_groups,
        observed_group_edges=obs_group_edges,
        observed_stage_to_group=obs_stage_to_group,
        # Alias of the observed set, kept so existing consumers keep working.
        groups=obs_groups,
        group_edges=obs_group_edges,
        stage_to_group=obs_stage_to_group,
        unit_kind=unit_kind,
        unit_id=unit_id,
    )
