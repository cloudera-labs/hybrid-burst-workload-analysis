import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.spark.model_parser import (
    EnvConstants,
    _State,
    build_results,
    process_event,
)

# input_read_gb uses DECIMAL GB (÷1e9) to match the transport-aware pipeline.
_GB = 10 ** 9


# --- event builders --------------------------------------------------------


def _app_start(name="my_app", app_id="application_1_1", ts=1_000_000):
    return {"Event": "SparkListenerApplicationStart", "App Name": name,
            "App ID": app_id, "Timestamp": ts}


def _app_end(ts=1_060_000):
    return {"Event": "SparkListenerApplicationEnd", "Timestamp": ts}


def _executor_added(exec_id, cores=2):
    return {"Event": "SparkListenerExecutorAdded", "Executor ID": exec_id,
            "Executor Info": {"Total Cores": cores}}


def _executor_removed(exec_id):
    return {"Event": "SparkListenerExecutorRemoved", "Executor ID": exec_id}


def _resource_profile(cores=4):
    return {"Event": "SparkListenerResourceProfileAdded",
            "Executor Resource Requests": {"cores": {"Amount": cores}}}


def _env_update(executor_memory="4G"):
    return {"Event": "SparkListenerEnvironmentUpdate",
            "Spark Properties": {"spark.executor.memory": executor_memory}}


def _stage_completed(stage_id=0, num_tasks=4, submission=1_000_000, completion=1_010_000,
                     runtime_ms=8000, cpu_ns=4_000_000_000, input_bytes=0,
                     shuffle_read_remote_bytes=0, shuffle_read_local_bytes=0,
                     rdd_scope_name=None):
    accumulables = [
        {"ID": 1, "Name": "internal.metrics.executorRunTime", "Value": runtime_ms},
        {"ID": 2, "Name": "internal.metrics.executorCpuTime", "Value": cpu_ns},
        {"ID": 3, "Name": "internal.metrics.input.bytesRead", "Value": input_bytes},
        {"ID": 6, "Name": "internal.metrics.shuffle.read.remoteBytesRead", "Value": shuffle_read_remote_bytes},
        {"ID": 7, "Name": "internal.metrics.shuffle.read.localBytesRead", "Value": shuffle_read_local_bytes},
        {"ID": 9, "Name": "number of output rows", "Value": 123},  # noise
    ]
    info = {"Stage ID": stage_id, "Number of Tasks": num_tasks,
            "Submission Time": submission, "Completion Time": completion,
            "Accumulables": accumulables}
    # RDD Info carries a Scope whose "name" links the stage to a plan node (the
    # fallback used to attribute driver-level scan metrics to a stage).
    if rdd_scope_name is not None:
        info["RDD Info"] = [{"Scope": '{"id":"1","name":"%s"}' % rdd_scope_name}]
    return {"Event": "SparkListenerStageCompleted", "Stage Info": info}


def _stage_submitted(stage_id=0, exec_id=0):
    """Links a stage to its SQL execution id (via Spark properties)."""
    return {"Event": "SparkListenerStageSubmitted",
            "Stage Info": {"Stage ID": stage_id},
            "Properties": {"spark.sql.execution.id": exec_id}}


def _driver_accum(exec_id=0, updates=None):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
            "executionId": exec_id, "accumUpdates": updates or []}


def _scan_node(table="db.t", file_acc_id=16, partition_acc_id=None):
    metrics = [{"accumulatorId": file_acc_id, "name": "number of files read"},
               {"accumulatorId": 15, "name": "number of output rows"}]
    if partition_acc_id is not None:
        metrics.append({"accumulatorId": partition_acc_id, "name": "number of partitions read"})
    return {"nodeName": "Scan parquet",
            "simpleString": f"FileScan parquet {table}[col#1]",
            "metrics": metrics, "children": []}


def _sql_start(scan_nodes=None):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": 0, "description": "sql at Foo.java:0",
            "sparkPlanInfo": {"nodeName": "Top", "simpleString": "Top", "metrics": [],
                              "children": scan_nodes or []}}


def _task_end(stage_id=0, accumulables=None):
    return {"Event": "SparkListenerTaskEnd", "Stage ID": stage_id,
            "Task Info": {"Accumulables": accumulables or []}}


def _run(events):
    state = _State()
    for e in events:
        process_event(e, state)
    return state


# --- metric extraction & unit conversion -----------------------------------


def test_stage_metric_conversions_decimal_gb():
    events = [
        _app_start(),
        _stage_completed(stage_id=0, num_tasks=4, submission=1_000_000, completion=1_010_000,
                         runtime_ms=8000, cpu_ns=4_000_000_000, input_bytes=2 * _GB),
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    s = stages[0]
    # No TaskEnd in these events, so there is no task span and the stage clock is
    # 0.0 - submission -> completion (10 s) is deliberately NOT what it reports.
    assert s.stage_duration_s == 0.0
    assert s.executor_runtime_s == 8.0
    assert s.executor_cpu_time_s == 4.0
    assert s.avg_task_duration_s == 2.0          # 8 s / 4 tasks
    assert s.input_read_gb == 2.0                # 2e9 bytes ÷ 1e9 = 2.0 (decimal GB)


def test_avg_task_duration_zero_tasks_no_division_error():
    _, stages = build_results(_run([_app_start(), _stage_completed(num_tasks=0), _app_end()]))
    assert stages[0].avg_task_duration_s == 0.0


def test_query_id_mirrors_app_id_per_run():
    job, stages = build_results(_run([_app_start(app_id="application_9_9"), _stage_completed(), _app_end()]))
    assert job.query_id == "application_9_9"
    assert stages[0].query_id == "application_9_9"
    assert stages[0].unique_stage_id == stages[0].stage_id


def test_no_app_start_yields_nothing():
    job, stages = build_results(_run([_stage_completed(), _app_end()]))
    assert job is None
    assert stages == []


# --- files / partitions per stage ------------------------------------------


def test_rdd_scan_stage_uses_task_count_proxy_for_files():
    events = [
        _app_start(),
        _stage_completed(stage_id=0, num_tasks=20, input_bytes=5 * _GB),                  # scan
        _stage_completed(stage_id=1, num_tasks=10, shuffle_read_remote_bytes=_GB),        # shuffle-only
        _stage_completed(stage_id=2, num_tasks=7),                                        # compute-only
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    by_id = {s.stage_id: s for s in stages}
    assert by_id[0].number_of_files_read == 20
    assert by_id[1].number_of_files_read is None
    assert by_id[2].number_of_files_read is None
    # Partitions falls back to files, as the published pipeline's does, so the
    # scan stage inherits the proxy and the two non-scan stages stay None.
    assert by_id[0].number_of_partitions_read == 20
    assert by_id[1].number_of_partitions_read is None
    assert by_id[2].number_of_partitions_read is None


def test_per_task_accumulator_attributes_files_and_partitions():
    # Primary mapping: a Spark version that pushes scan counts per task. The stage is
    # linked to its execution via StageSubmitted, so the per-task sums land on stage 0.
    events = [
        _app_start(),
        _sql_start(scan_nodes=[_scan_node(file_acc_id=16, partition_acc_id=30)]),
        _stage_submitted(stage_id=0, exec_id=0),
        _task_end(stage_id=0, accumulables=[{"ID": 16, "Update": 40}, {"ID": 30, "Update": 8}]),
        _task_end(stage_id=0, accumulables=[{"ID": 16, "Update": 60}, {"ID": 30, "Update": 12}]),
        _stage_completed(stage_id=0, num_tasks=4, input_bytes=2 * _GB),
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    assert stages[0].number_of_files_read == 100     # per-task acc sum
    assert stages[0].number_of_partitions_read == 20


def test_driver_value_attributed_via_node_name_fallback():
    # The common production case: the scan counts are driver-only (not per task). The
    # value comes from DriverAccumUpdates; the stage is found by matching the plan node
    # name ("Scan parquet") to the stage's RDD Info Scope name.
    events = [
        _app_start(),
        _sql_start(scan_nodes=[_scan_node(file_acc_id=16, partition_acc_id=30)]),
        _stage_submitted(stage_id=0, exec_id=0),
        _stage_completed(stage_id=0, num_tasks=4, input_bytes=2 * _GB,
                         rdd_scope_name="Scan parquet"),
        _driver_accum(exec_id=0, updates=[[16, 500], [30, 40]]),
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    assert stages[0].number_of_files_read == 500          # driver value, node-name fallback
    assert stages[0].static_number_of_files_read == 500   # backfilled from files
    assert stages[0].number_of_partitions_read == 40


def test_unresolved_partitions_fall_back_to_the_file_count():
    # SQL scan present but no driver value, no per-task accumulables, and no RDD-scope
    # match. Files falls back to the scan-task proxy, and partitions then falls back
    # to files - which is what the published pipeline does (parse_queries.py:239-243)
    # and what makes the two campaigns attribute the metric to the same stages.
    events = [
        _app_start(),
        _sql_start(scan_nodes=[_scan_node(file_acc_id=16, partition_acc_id=30)]),
        _stage_submitted(stage_id=0, exec_id=0),
        _stage_completed(stage_id=0, num_tasks=4, input_bytes=2 * _GB),
        _app_end(),
    ]
    _, stages = build_results(_run(events))
    assert stages[0].number_of_files_read == 4
    assert stages[0].number_of_partitions_read == 4


# --- job aggregation -------------------------------------------------------


def test_job_aggregates_and_metadata():
    events = [
        _app_start(name="agg", app_id="application_1_42", ts=1_000_000),
        _resource_profile(cores=4),
        _env_update(executor_memory="8G"),
        _executor_added("1"), _executor_added("2"),
        _stage_completed(stage_id=0, input_bytes=2 * _GB, runtime_ms=3000),
        _stage_completed(stage_id=1, input_bytes=3 * _GB, runtime_ms=5000),
        _app_end(ts=1_060_000),
    ]
    job, stages = build_results(_run(events))
    assert job.app_id == "application_1_42"
    assert job.spark_executor_cores == 4
    assert job.spark_executor_memory_gb == 8.0
    assert job.spark_num_executors == 2
    assert job.exec_time_s == 60.0
    assert job.num_spark_stages == 2
    assert job.spark_total_input_read_gb == 5.0     # decimal GB
    assert job.spark_total_executor_runtime_s == 8.0
    assert len(stages) == 2
    assert stages[0].spark_num_executors == 2
    assert stages[0].total_stages == 2


def test_num_executors_is_peak_concurrent_not_cumulative():
    events = [
        _app_start(),
        _executor_added("1"), _executor_added("2"), _executor_added("3"),
        _executor_removed("1"), _executor_removed("2"),
        _executor_added("4"), _executor_added("5"),
        _app_end(),
    ]
    job, _ = build_results(_run(events))
    assert job.spark_num_executors == 3


def test_stages_sorted_by_id():
    events = [_app_start(), _stage_completed(stage_id=5), _stage_completed(stage_id=2),
              _stage_completed(stage_id=11), _app_end()]
    _, stages = build_results(_run(events))
    assert [s.stage_id for s in stages] == [2, 5, 11]


# --- environment constants -------------------------------------------------


def test_env_constants_null_by_default():
    _, stages = build_results(_run([_app_start(), _stage_completed(), _app_end()]))
    s = stages[0]
    assert s.window_max_bytes is None
    assert s.mtu is None
    assert s.network_latency_ms is None
    assert s.tsg_max_bw_gbs is None


def test_env_constants_stamped_on_every_row():
    env = EnvConstants(window_max_bytes=134217728, mtu=9001,
                       network_latency_ms=10.0, tsg_max_bw_gbs=25.0)
    events = [_app_start(), _stage_completed(stage_id=0), _stage_completed(stage_id=1), _app_end()]
    job, stages = build_results(_run(events), env)
    for s in stages:
        assert s.window_max_bytes == 134217728
        assert s.mtu == 9001
        assert s.network_latency_ms == 10.0
        assert s.tsg_max_bw_gbs == 25.0
    assert job.network_latency_ms == 10.0
    assert job.tsg_max_bw_gbs == 25.0
    assert env.missing() == []


def test_env_constants_missing_reports_unset():
    assert set(EnvConstants().missing()) == {
        "window_max_bytes", "mtu", "network_latency_ms", "tsg_max_bw_gbs"}
    assert EnvConstants(mtu=1500).missing() == [
        "window_max_bytes", "network_latency_ms", "tsg_max_bw_gbs"]
