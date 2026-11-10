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

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.spark.event_parser import (
    _ParserState,
    _compute_match_id,
    _normalize_name,
    _parse_memory_gb,
    _resolve_file_counts,
    build_results,
    process_event,
)

# Time conversion bases used throughout: 1000 ms = 1 s, 1e9 ns = 1 s, 1024**3 bytes = 1 GB.
_GB = 1024 ** 3


# --- event builders (mirror the real NDJSON event shapes) -----------------


def _app_start(name="my_app", app_id="application_1_1", user="airflow", ts=1_000_000):
    return {
        "Event": "SparkListenerApplicationStart",
        "App Name": name,
        "App ID": app_id,
        "User": user,
        "Timestamp": ts,
    }


def _app_end(ts=1_060_000):
    return {"Event": "SparkListenerApplicationEnd", "Timestamp": ts}


def _log_start(version="3.3.2.test"):
    return {"Event": "SparkListenerLogStart", "Spark Version": version}


def _executor_added(exec_id, cores=2):
    return {
        "Event": "SparkListenerExecutorAdded",
        "Executor ID": exec_id,
        "Executor Info": {"Total Cores": cores},
    }


def _executor_removed(exec_id):
    return {"Event": "SparkListenerExecutorRemoved", "Executor ID": exec_id}


def _resource_profile(cores=4, memory_mb=4096):
    return {
        "Event": "SparkListenerResourceProfileAdded",
        "Executor Resource Requests": {
            "cores": {"Amount": cores},
            "memory": {"Amount": memory_mb},
        },
    }


def _env_update(executor_memory="4G"):
    return {
        "Event": "SparkListenerEnvironmentUpdate",
        "Spark Properties": {"spark.executor.memory": executor_memory},
    }


def _stage_completed(
    stage_id=0,
    num_tasks=4,
    submission=1_000_000,
    completion=1_010_000,
    runtime_ms=8000,
    cpu_ns=4_000_000_000,
    input_bytes=0,
    output_bytes=0,
    shuffle_write_bytes=0,
    shuffle_read_remote_bytes=0,
    shuffle_read_local_bytes=0,
):
    accumulables = [
        {"ID": 1, "Name": "internal.metrics.executorRunTime", "Value": runtime_ms},
        {"ID": 2, "Name": "internal.metrics.executorCpuTime", "Value": cpu_ns},
        {"ID": 3, "Name": "internal.metrics.input.bytesRead", "Value": input_bytes},
        {"ID": 4, "Name": "internal.metrics.output.bytesWritten", "Value": output_bytes},
        {"ID": 5, "Name": "internal.metrics.shuffle.write.bytesWritten", "Value": shuffle_write_bytes},
        {"ID": 6, "Name": "internal.metrics.shuffle.read.remoteBytesRead", "Value": shuffle_read_remote_bytes},
        {"ID": 7, "Name": "internal.metrics.shuffle.read.localBytesRead", "Value": shuffle_read_local_bytes},
        # Noise accumulables that must be ignored:
        {"ID": 8, "Name": "internal.metrics.jvmGCTime", "Value": 999},
        {"ID": 9, "Name": "number of output rows", "Value": 123},
    ]
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": stage_id,
            "Number of Tasks": num_tasks,
            "Submission Time": submission,
            "Completion Time": completion,
            "Accumulables": accumulables,
        },
    }


def _task_start(stage_id=0, launch=1_000_000):
    return {
        "Event": "SparkListenerTaskStart",
        "Stage ID": stage_id,
        "Task Info": {"Launch Time": launch},
    }


def _scan_node(table, file_acc_id=16, partition_acc_id=None, size_acc_id=18):
    """A FileScan plan node with the file/size accumulators seen in real logs."""
    metrics = [
        {"accumulatorId": file_acc_id, "name": "number of files read"},
        {"accumulatorId": size_acc_id, "name": "size of files read"},
        {"accumulatorId": 15, "name": "number of output rows"},
        {"accumulatorId": 19, "name": "scan time"},
    ]
    if partition_acc_id is not None:
        metrics.append({"accumulatorId": partition_acc_id, "name": "number of partitions read"})
    return {
        "nodeName": "Scan parquet",
        "simpleString": f"FileScan parquet {table}[col#1] Batched: true",
        "metrics": metrics,
        "children": [],
    }


def _sql_start(exec_id=0, description="sql at Foo.java:0", scan_nodes=None):
    children = scan_nodes or []
    return {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": exec_id,
        "description": description,
        "sparkPlanInfo": {
            "nodeName": "TopNode",
            "simpleString": "TopNode",
            "metrics": [],
            "children": children,
        },
    }


def _driver_accum(exec_id=0, updates=None):
    return {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
        "executionId": exec_id,
        "accumUpdates": updates or [],
    }


def _run(events):
    state = _ParserState()
    for e in events:
        process_event(e, state)
    return state


# --- normalization & match_id ---------------------------------------------


def test_normalize_name_lowercases_and_collapses():
    assert _normalize_name("HDFS to Hive") == "hdfs_to_hive"
    assert _normalize_name("Foo.Bar_Baz-123") == "foo_bar_baz_123"
    assert _normalize_name("  leading/trailing  ") == "leading_trailing"


def test_match_id_rdd_is_plain_normalized_name():
    state = _run([_app_start(name="My RDD Job"), _app_end()])
    # No SQL events -> rdd -> no fingerprint suffix
    assert _compute_match_id(state) == "my_rdd_job"


def test_match_id_sql_has_fingerprint_suffix():
    events = [
        _app_start(name="HDFS to Hive"),
        _sql_start(scan_nodes=[_scan_node("db.tbl_a")]),
        _app_end(),
    ]
    state = _run(events)
    match_id = _compute_match_id(state)
    assert match_id.startswith("hdfs_to_hive_")
    assert len(match_id.split("_")[-1]) == 8  # 8-hex fingerprint


def test_match_id_same_tables_same_fingerprint():
    a = _run([_app_start(name="HDFS to Hive"), _sql_start(scan_nodes=[_scan_node("db.tbl_a")]), _app_end()])
    b = _run([_app_start(name="HDFS to Hive"), _sql_start(scan_nodes=[_scan_node("db.tbl_a")]), _app_end()])
    assert _compute_match_id(a) == _compute_match_id(b)


def test_match_id_different_tables_different_fingerprint():
    a = _run([_app_start(name="HDFS to Hive"), _sql_start(scan_nodes=[_scan_node("db.tbl_a")]), _app_end()])
    b = _run([_app_start(name="HDFS to Hive"), _sql_start(scan_nodes=[_scan_node("db.tbl_b")]), _app_end()])
    assert _compute_match_id(a) != _compute_match_id(b)


def test_match_id_ignores_versioned_table_batch_suffix():
    # Production pipelines materialize batch-versioned staging tables each run
    # (account_detail_20948 -> account_detail_20949). Different batch suffix, same workload.
    a = _run([_app_start(name="viper"), _sql_start(scan_nodes=[_scan_node("stg.account_detail_20948")]), _app_end()])
    b = _run([_app_start(name="viper"), _sql_start(scan_nodes=[_scan_node("stg.account_detail_20949")]), _app_end()])
    assert _compute_match_id(a) == _compute_match_id(b)


def test_match_id_ignores_streaming_runid_and_batch():
    # Structured Streaming jobs scan no tables; their description embeds a per-run
    # runId and batch number. Same stable streaming id -> same group.
    desc1 = "\nid = 48542dd8-6fd1-4009-a9e6-c4b69dd5c275\nrunId = 5b6bc44b-ea07-4e57-a57c-077c8fc0920c\nbatch = 7400"
    desc2 = "\nid = 48542dd8-6fd1-4009-a9e6-c4b69dd5c275\nrunId = 7e9f67f2-1ed0-41d4-8693-be7b0dd22c2a\nbatch = 7401"
    a = _run([_app_start(name="Process manowar"), _sql_start(description=desc1), _app_end()])
    b = _run([_app_start(name="Process manowar"), _sql_start(description=desc2), _app_end()])
    assert _compute_match_id(a) == _compute_match_id(b)


def test_match_id_distinct_streaming_ids_stay_separate():
    # The stable streaming query id is preserved, so two different streaming queries
    # behind the same app name remain distinct groups.
    desc1 = "\nid = 11111111-1111-1111-1111-111111111111\nrunId = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\nbatch = 1"
    desc2 = "\nid = 22222222-2222-2222-2222-222222222222\nrunId = bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\nbatch = 1"
    a = _run([_app_start(name="Process manowar"), _sql_start(description=desc1), _app_end()])
    b = _run([_app_start(name="Process manowar"), _sql_start(description=desc2), _app_end()])
    assert _compute_match_id(a) != _compute_match_id(b)


# --- type detection --------------------------------------------------------


def test_type_is_rdd_without_sql_events():
    job, _ = build_results(_run([_app_start(), _stage_completed(), _app_end()]))
    assert job.type == "rdd"


def test_type_is_sql_with_sql_execution_start():
    events = [_app_start(), _sql_start(scan_nodes=[_scan_node("db.t")]), _stage_completed(), _app_end()]
    job, _ = build_results(_run(events))
    assert job.type == "sql"


# --- SQL file/partition counts (regression: the size-vs-count bug) ---------


def test_sql_file_count_from_driver_accum():
    events = [
        _app_start(),
        _sql_start(scan_nodes=[_scan_node("db.t", file_acc_id=16, size_acc_id=18)]),
        # acc 16 = files (178982), acc 18 = size in bytes (must NOT be counted)
        _driver_accum(updates=[[16, 178982], [17, 380], [18, 2_497_230_012]]),
        _app_end(),
    ]
    files, partitions = _resolve_file_counts(_run(events))
    assert files == 178982  # not the multi-GB size accumulator
    assert partitions is None


def test_sql_size_accumulator_never_counted_as_files():
    # Only the size accumulator reports a value; file accumulator is absent from updates.
    events = [
        _app_start(),
        _sql_start(scan_nodes=[_scan_node("db.t", file_acc_id=16, size_acc_id=18)]),
        _driver_accum(updates=[[18, 9_999_999_999]]),
        _app_end(),
    ]
    files, _ = _resolve_file_counts(_run(events))
    assert files is None  # size value must not leak into file count


def test_sql_partition_count_distinct_from_number_of_partitions():
    # Node exposes "number of partitions read" (acc 30); a sibling "number of partitions"
    # metric must not be picked up.
    scan = _scan_node("db.t", file_acc_id=16, partition_acc_id=30)
    scan["metrics"].append({"accumulatorId": 31, "name": "number of partitions"})
    events = [
        _app_start(),
        _sql_start(scan_nodes=[scan]),
        _driver_accum(updates=[[16, 10], [30, 7], [31, 999]]),
        _app_end(),
    ]
    files, partitions = _resolve_file_counts(_run(events))
    assert files == 10
    assert partitions == 7


def test_sql_file_count_sums_multiple_scan_nodes():
    events = [
        _app_start(),
        _sql_start(scan_nodes=[
            _scan_node("db.a", file_acc_id=16),
            _scan_node("db.b", file_acc_id=26),
        ]),
        _driver_accum(updates=[[16, 100], [26, 50]]),
        _app_end(),
    ]
    files, _ = _resolve_file_counts(_run(events))
    assert files == 150


# --- RDD scan-task proxy ---------------------------------------------------


def test_rdd_proxy_counts_only_scan_stages():
    events = [
        _app_start(name="rdd job"),
        _stage_completed(stage_id=0, num_tasks=20, input_bytes=5 * _GB),   # scan stage -> counted
        _stage_completed(stage_id=1, num_tasks=10, shuffle_read_remote_bytes=_GB),  # shuffle -> excluded
        _stage_completed(stage_id=2, num_tasks=7),                          # compute only -> excluded
        _app_end(),
    ]
    files, partitions = _resolve_file_counts(_run(events))
    assert files == 20
    assert partitions is None


def test_rdd_proxy_excludes_scan_stage_with_shuffle_read():
    # A stage that both reads input and shuffle-reads is not a pure scan -> excluded.
    events = [
        _app_start(name="rdd job"),
        _stage_completed(stage_id=0, num_tasks=20, input_bytes=_GB, shuffle_read_local_bytes=_GB),
        _app_end(),
    ]
    files, _ = _resolve_file_counts(_run(events))
    assert files is None


def test_rdd_per_stage_file_proxy_populated_for_scan_stages_only():
    events = [
        _app_start(name="rdd job"),
        _stage_completed(stage_id=0, num_tasks=20, input_bytes=5 * _GB),                 # scan
        _stage_completed(stage_id=1, num_tasks=10, shuffle_read_remote_bytes=_GB),       # shuffle-only
        _stage_completed(stage_id=2, num_tasks=7),                                       # compute-only
        _app_end(),
    ]
    job, stages = build_results(_run(events))
    by_id = {s.stage_id: s for s in stages}
    assert by_id[0].number_of_files_read == 20   # scan -> task-count proxy
    assert by_id[1].number_of_files_read is None  # shuffle-only -> not a scan
    assert by_id[2].number_of_files_read is None  # compute-only -> not a scan
    # Per-stage proxies sum to the job-level total.
    assert job.total_files_read == 20


def test_sql_per_stage_file_count_is_none_but_job_total_set():
    # SQL file counts come from driver accumulators (query-level) and cannot be
    # attributed per stage -> stage field stays None while the job total is exact.
    events = [
        _app_start(name="sql job"),
        _sql_start(scan_nodes=[_scan_node("db.t", file_acc_id=16)]),
        _stage_completed(stage_id=0, num_tasks=4, input_bytes=2 * _GB),
        _driver_accum(updates=[[16, 4321]]),
        _app_end(),
    ]
    job, stages = build_results(_run(events))
    assert stages[0].number_of_files_read is None
    assert job.total_files_read == 4321


# --- metric extraction & unit conversion -----------------------------------


def test_stage_metric_conversions():
    events = [
        _app_start(),
        _stage_completed(
            stage_id=0,
            num_tasks=4,
            submission=1_000_000,
            completion=1_010_000,        # 10 s
            runtime_ms=8000,             # 8 s
            cpu_ns=4_000_000_000,        # 4 s
            input_bytes=2 * _GB,
            output_bytes=_GB,
            shuffle_write_bytes=_GB,
            shuffle_read_remote_bytes=_GB,
            shuffle_read_local_bytes=_GB,
        ),
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    s = stages[0]
    assert s.stage_duration_s == 10.0
    assert s.executor_runtime_s == 8.0
    assert s.executor_cpu_time_s == 4.0
    assert s.avg_task_duration_s == 2.0          # 8 s / 4 tasks
    assert s.input_read_gb == 2.0
    assert s.output_write_gb == 1.0
    assert s.shuffle_write_gb == 1.0
    assert s.shuffle_read_gb == 2.0              # remote + local


def test_avg_task_duration_zero_tasks_no_division_error():
    events = [_app_start(), _stage_completed(num_tasks=0, runtime_ms=5000), _app_end()]
    _, stages = build_results(_run(events))
    assert stages[0].avg_task_duration_s == 0.0


# --- job aggregation & metadata --------------------------------------------


def test_job_aggregates_across_stages():
    events = [
        _log_start(version="3.3.2.test"),
        _app_start(name="agg", app_id="application_1_42", user="airflow", ts=1_000_000),
        _resource_profile(cores=4),
        _env_update(executor_memory="8G"),
        _executor_added("1"),
        _executor_added("2"),
        _stage_completed(stage_id=0, input_bytes=2 * _GB, runtime_ms=3000),
        _stage_completed(stage_id=1, input_bytes=3 * _GB, runtime_ms=5000),
        _app_end(ts=1_060_000),          # 60 s
    ]
    job, stages = build_results(_run(events))
    assert job.app_id == "application_1_42"
    assert job.query_id == "application_1_42"        # query_id mirrors app_id for the loader
    assert job.spark_user == "airflow"
    assert job.spark_version == "3.3.2.test"
    assert job.spark_executor_cores == 4
    assert job.spark_executor_memory_gb == 8.0        # "8G" -> 8.0 GB
    assert job.spark_num_executors == 2
    assert job.exec_time_s == 60.0
    assert job.num_spark_stages == 2
    assert job.spark_total_input_read_gb == 5.0
    assert job.spark_total_executor_runtime_s == 8.0
    assert len(stages) == 2
    # Stage rows carry the executor parallelism the stage loader needs.
    assert stages[0].spark_num_executors == 2
    assert stages[0].spark_executor_cores == 4
    assert stages[0].total_stages == 2


def test_stages_sorted_by_id():
    events = [
        _app_start(),
        _stage_completed(stage_id=5),
        _stage_completed(stage_id=2),
        _stage_completed(stage_id=11),
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    assert [s.stage_id for s in stages] == [2, 5, 11]


def test_active_exec_time_is_stage_span_bounded_by_wallclock():
    # Wall-clock spans 70s (start 990_000 → end 1_060_000), but stages only run
    # from first submission (1_000_000) to last completion (1_020_000) = 20s.
    events = [
        _app_start(ts=990_000),
        _stage_completed(stage_id=0, submission=1_000_000, completion=1_010_000),
        _stage_completed(stage_id=1, submission=1_005_000, completion=1_020_000),
        _app_end(ts=1_060_000),
    ]
    job, _ = build_results(_run(events))
    assert job.exec_time_s == 70.0           # wall-clock
    assert job.active_exec_time_s == 20.0    # stage span, strictly less
    assert job.active_exec_time_s <= job.exec_time_s


def test_active_exec_time_zero_without_stage_timestamps():
    job, _ = build_results(_run([_app_start(ts=1_000_000), _app_end(ts=1_060_000)]))
    assert job.active_exec_time_s == 0.0


def test_contention_tax_sums_per_stage_executor_acquisition_wait():
    # Stage 0: submitted at 1_000_000, first task launched 3 s later (executor arrived late).
    # Stage 1: submitted at 1_005_000, first task launched immediately -> 0 wait.
    # contention_tax = 3 s + 0 s. It lives inside the active span, not in overhead.
    events = [
        _app_start(ts=990_000),
        _task_start(stage_id=0, launch=1_003_000),
        _stage_completed(stage_id=0, submission=1_000_000, completion=1_010_000),
        _task_start(stage_id=1, launch=1_005_000),
        _stage_completed(stage_id=1, submission=1_005_000, completion=1_020_000),
        _app_end(ts=1_060_000),
    ]
    job, _ = build_results(_run(events))
    assert job.contention_tax_s == 3.0
    # Sits inside active (stage span), so it is not the same as overhead.
    assert job.contention_tax_s <= job.active_exec_time_s


def test_contention_tax_takes_earliest_task_per_stage():
    # Two tasks in the same stage; the FIRST launch (not the later one) sets the wait.
    events = [
        _app_start(ts=990_000),
        _task_start(stage_id=0, launch=1_008_000),   # later task
        _task_start(stage_id=0, launch=1_002_000),   # earliest -> 2 s wait
        _stage_completed(stage_id=0, submission=1_000_000, completion=1_010_000),
        _app_end(ts=1_060_000),
    ]
    job, _ = build_results(_run(events))
    assert job.contention_tax_s == 2.0


def test_contention_tax_zero_without_task_events():
    # No SparkListenerTaskStart -> nothing to measure the wait from.
    events = [_app_start(), _stage_completed(stage_id=0), _app_end()]
    job, _ = build_results(_run(events))
    assert job.contention_tax_s == 0.0


def _executor_added_at(exec_id, ts, cores=2):
    """ExecutorAdded carrying a Timestamp (the plain helper omits it, so resource_wait
    stays 0 in tests that don't care about it)."""
    e = _executor_added(exec_id, cores=cores)
    e["Timestamp"] = ts
    return e


def test_resource_wait_is_time_until_first_executor():
    # App admitted at t=0; the first executor arrives 30 s later; stages run only after
    # that. The 30 s before any executor existed is pure resource wait, and it fits inside
    # overhead (wall 60 s − active 20 s = 40 s), so it is reported in full.
    events = [
        _app_start(ts=1_000_000),
        _executor_added_at("1", ts=1_030_000),
        _stage_completed(stage_id=0, submission=1_040_000, completion=1_060_000),
        _app_end(ts=1_060_000),
    ]
    job, _ = build_results(_run(events))
    assert job.resource_wait_s == 30.0
    # Never exceeds overhead, so the three buckets still partition wall-clock.
    assert job.resource_wait_s <= job.exec_time_s - job.active_exec_time_s


def test_resource_wait_clamped_to_overhead():
    # Dynamic-allocation cold start: the first stage is submitted (active clock starts) at
    # t=10 s, BEFORE the first executor arrives at t=30 s. raw wait = 30 s but overhead is
    # only 20 s (wall 60 − active 40), so the clamp caps resource_wait at 20 s; the
    # in-active remainder is left to contention, never double-counted.
    events = [
        _app_start(ts=1_000_000),
        _stage_completed(stage_id=0, submission=1_010_000, completion=1_050_000),
        _executor_added_at("1", ts=1_030_000),
        _app_end(ts=1_060_000),
    ]
    job, _ = build_results(_run(events))
    assert job.exec_time_s - job.active_exec_time_s == 20.0
    assert job.resource_wait_s == 20.0


def test_resource_wait_zero_without_executor_added():
    # No ExecutorAdded event (truncated log / app never scheduled) -> we do not claim the
    # wall-clock as resource wait; it falls into planning/startup instead.
    events = [
        _app_start(ts=1_000_000),
        _stage_completed(stage_id=0, submission=1_010_000, completion=1_050_000),
        _app_end(ts=1_060_000),
    ]
    job, _ = build_results(_run(events))
    assert job.resource_wait_s == 0.0


def test_missing_application_end_yields_zero_duration():
    job, _ = build_results(_run([_app_start(ts=1_000_000), _stage_completed()]))
    assert job.exec_time_s == 0.0


def test_num_executors_is_peak_concurrent_not_cumulative():
    # Dynamic allocation churn: 3 added, 2 removed, 2 more added.
    # Cumulative distinct IDs = 5, but peak concurrent = 3.
    events = [
        _app_start(name="dyn"),
        _executor_added("1"), _executor_added("2"), _executor_added("3"),  # live=3 (peak)
        _executor_removed("1"), _executor_removed("2"),                     # live=1
        _executor_added("4"), _executor_added("5"),                         # live=3
        _app_end(),
    ]
    job, _ = build_results(_run(events))
    assert job.spark_num_executors == 3   # peak, not 5


def test_executor_cores_fallback_to_executor_added():
    # No ResourceProfileAdded -> cores come from ExecutorAdded.Total Cores
    events = [_app_start(), _executor_added("1", cores=3), _app_end()]
    job, _ = build_results(_run(events))
    assert job.spark_executor_cores == 3


# --- dispatcher robustness -------------------------------------------------


def test_process_event_ignores_unknown_and_malformed():
    state = _ParserState()
    process_event({"Event": "SparkListenerTaskEnd"}, state)   # unhandled, no-op
    process_event({"Event": "TotallyUnknown"}, state)
    process_event({}, state)                                   # no Event key
    assert state.stages == {}


# --- memory parsing --------------------------------------------------------


def test_parse_memory_gb():
    assert _parse_memory_gb("4G") == 4.0
    assert _parse_memory_gb("2g") == 2.0
    assert _parse_memory_gb("4096m") == 4.0       # MiB -> GiB
    assert _parse_memory_gb("1t") == 1024.0
    assert _parse_memory_gb("") == 0.0            # missing -> 0
    assert _parse_memory_gb("garbage") == 0.0
