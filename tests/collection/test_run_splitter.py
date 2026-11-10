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

import json
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.spark.model_parser import (
    EnvConstants,
    _State,
    build_graph,
    build_results,
    process_event,
)
from src.collection.spark.run_splitter import (
    SPLIT_NONE,
    SPLIT_SQL,
    Sidecar,
    split_state,
)
from src.collection.spark.tpcds_sidecar import extract_sidecar

_GB = 10 ** 9


# --- event builders --------------------------------------------------------


def _app_start(app_id="application_1_1", ts=1_000_000):
    return {"Event": "SparkListenerApplicationStart", "App Name": "bench",
            "App ID": app_id, "Timestamp": ts}


def _app_end(ts=1_100_000):
    return {"Event": "SparkListenerApplicationEnd", "Timestamp": ts}


def _executor_added(exec_id="1", ts=1_000_500, cores=4):
    return {"Event": "SparkListenerExecutorAdded", "Executor ID": exec_id,
            "Timestamp": ts, "Executor Info": {"Total Cores": cores}}


def _stage_submitted(stage_id, exec_id):
    return {"Event": "SparkListenerStageSubmitted",
            "Stage Info": {"Stage ID": stage_id},
            "Properties": {"spark.sql.execution.id": exec_id}}


def _stage_completed(stage_id, submission, completion, input_bytes=0, parents=None,
                     runtime_ms=1000):
    return {"Event": "SparkListenerStageCompleted", "Stage Info": {
        "Stage ID": stage_id, "Number of Tasks": 2,
        "Submission Time": submission, "Completion Time": completion,
        "Parent IDs": parents or [],
        "Accumulables": [
            {"ID": 1, "Name": "internal.metrics.executorRunTime", "Value": runtime_ms},
            {"ID": 3, "Name": "internal.metrics.input.bytesRead", "Value": input_bytes},
        ]}}


def _sql_start(exec_id, ts):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
            "executionId": exec_id, "description": "foreach at Bench.java:89", "time": ts}


def _sql_end(exec_id, ts):
    return {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
            "executionId": exec_id, "time": ts}


def _two_query_app():
    """One application holding two SQL executions, plus a third that made no stages."""
    return [
        _app_start(ts=1_000_000),
        _executor_added(ts=1_000_500),
        # execution 0: the driver's opening statement. No stages.
        _sql_start(0, 1_000_600), _sql_end(0, 1_000_700),
        # execution 1: two stages, one depending on the other.
        _sql_start(1, 1_001_000),
        _stage_submitted(10, 1), _stage_submitted(11, 1),
        _stage_completed(10, 1_001_100, 1_002_100, input_bytes=3 * _GB),
        _stage_completed(11, 1_002_100, 1_003_100, parents=[10]),
        _sql_end(1, 1_003_200),
        # execution 2: one stage.
        _sql_start(2, 1_004_000),
        _stage_submitted(20, 2),
        _stage_completed(20, 1_004_100, 1_006_100, input_bytes=7 * _GB),
        _sql_end(2, 1_006_200),
        _app_end(ts=1_010_000),
    ]


def _run(events):
    state = _State()
    for event in events:
        process_event(event, state)
    return state


# --- the default split is the old behaviour --------------------------------


def test_split_none_returns_the_whole_application():
    state = _run(_two_query_app())
    units = split_state(state, SPLIT_NONE)
    assert len(units) == 1
    unit, sub = units[0]
    assert sub is state
    assert unit.query_id == "application_1_1"
    assert unit.unit_kind == "application"


def test_split_none_job_matches_the_unsplit_parse():
    """The default path must be byte-identical to a parse that never split."""
    state = _run(_two_query_app())
    plain_job, plain_stages = build_results(state, EnvConstants())
    (unit, sub), = split_state(state, SPLIT_NONE)
    split_job, split_stages = build_results(sub, EnvConstants(), unit)
    assert split_job == plain_job
    assert split_stages == plain_stages


# --- the sql split ---------------------------------------------------------


def test_split_sql_makes_one_unit_per_execution_that_ran_stages():
    state = _run(_two_query_app())
    units = split_state(state, SPLIT_SQL)
    assert [u.unit_id for u, _ in units] == ["1", "2"]
    assert [sorted(u.stage_ids) for u, _ in units] == [[10, 11], [20]]


def test_split_sql_partitions_every_stage_exactly_once():
    state = _run(_two_query_app())
    seen = [s for u, _ in split_state(state, SPLIT_SQL) for s in u.stage_ids]
    assert sorted(seen) == sorted(state.stages)
    assert len(seen) == len(set(seen))


def test_split_sql_scopes_the_metrics_to_the_unit():
    state = _run(_two_query_app())
    units = dict((u.unit_id, sub) for u, sub in split_state(state, SPLIT_SQL))
    first, _ = build_results(units["1"], EnvConstants())
    second, _ = build_results(units["2"], EnvConstants())
    assert first.spark_total_input_read_gb == pytest.approx(3.0)
    assert second.spark_total_input_read_gb == pytest.approx(7.0)
    assert first.num_spark_stages == 2
    assert second.num_spark_stages == 1


def test_split_sql_uses_the_sql_span_as_the_wall_clock():
    state = _run(_two_query_app())
    units = split_state(state, SPLIT_SQL)
    jobs = [build_results(sub, EnvConstants(), unit)[0] for unit, sub in units]
    assert jobs[0].wall_clock_s == pytest.approx(2.2)   # 1_001_000 -> 1_003_200
    assert jobs[1].wall_clock_s == pytest.approx(2.2)   # 1_004_000 -> 1_006_200


def test_only_the_first_unit_can_carry_resource_wait():
    """Executors are already held once the first query has run; the wait is not re-charged."""
    state = _run(_two_query_app())
    units = split_state(state, SPLIT_SQL)
    jobs = [build_results(sub, EnvConstants(), unit)[0] for unit, sub in units]
    assert all(j.resource_wait_s == 0.0 for j in jobs)


def test_the_wall_clock_parts_still_sum_for_a_unit():
    state = _run(_two_query_app())
    for unit, sub in split_state(state, SPLIT_SQL):
        job, _ = build_results(sub, EnvConstants(), unit)
        active = min(job.stage_span_s, job.wall_clock_s)
        assert job.resource_wait_s + job.planning_startup_s + active == pytest.approx(
            job.wall_clock_s, abs=0.01)


def test_a_stage_span_crossing_the_sql_boundary_is_clamped_not_hidden():
    """A stage carrying this execution's id can be submitted before the SQL start.

    Clamping keeps the three wall-clock parts summing; ``stage_span_s`` keeps the
    raw value on the record so the straddle is still visible.
    """
    events = [
        _app_start(ts=1_000_000), _executor_added(ts=1_000_100),
        _sql_start(1, 1_002_000),
        _stage_submitted(10, 1),
        _stage_completed(10, 1_001_000, 1_003_000),   # submitted 1 s before the SQL start
        _sql_end(1, 1_003_100),
        _app_end(ts=1_004_000),
    ]
    (unit, sub), = split_state(_run(events), SPLIT_SQL)
    job, _ = build_results(sub, EnvConstants(), unit)
    assert job.wall_clock_s == pytest.approx(1.1)
    assert job.stage_span_s == pytest.approx(2.0)      # the raw span is preserved
    assert job.planning_startup_s == 0.0               # active was clamped to the wall


def test_each_unit_gets_its_own_graph():
    state = _run(_two_query_app())
    graphs = [build_graph(sub, unit) for unit, sub in split_state(state, SPLIT_SQL)]
    assert [g.stages for g in graphs] == [[10, 11], [20]]
    assert graphs[0].stage_edges == [[10, 11]]
    assert graphs[1].stage_edges == []
    assert {g.app_id for g in graphs} == {"application_1_1"}


def test_an_unknown_split_mode_is_refused():
    with pytest.raises(ValueError, match="unknown split mode"):
        split_state(_run(_two_query_app()), "per-job")


# --- labels ----------------------------------------------------------------


def test_the_sidecar_names_the_units_from_a_stage_map():
    state = _run(_two_query_app())
    sidecar = Sidecar.from_dict({"by_stage": {"10": "query1", "11": "query1", "20": "query2"}})
    units = split_state(state, SPLIT_SQL, sidecar)
    assert [u.query_id for u, _ in units] == ["query1", "query2"]
    assert all(u.labelled for u, _ in units)


def test_the_sidecar_names_the_units_from_an_exec_map():
    state = _run(_two_query_app())
    sidecar = Sidecar.from_dict({"by_exec": {"1": "query1", "2": "query2"}})
    assert [u.query_id for u, _ in split_state(state, SPLIT_SQL, sidecar)] == ["query1", "query2"]


def test_a_bare_mapping_is_read_as_a_stage_map():
    sidecar = Sidecar.from_dict({"10": "query1"})
    assert sidecar.by_stage == {10: "query1"}


def test_an_unnamed_unit_gets_a_traceable_key_not_a_guess():
    state = _run(_two_query_app())
    units = split_state(state, SPLIT_SQL)
    assert [u.query_id for u, _ in units] == [
        "application_1_1#exec1", "application_1_1#exec2"]
    assert not any(u.labelled for u, _ in units)


def test_a_stage_map_that_contradicts_the_log_is_reported_not_resolved():
    """Two query names inside one SQL execution means the map and the log disagree."""
    state = _run(_two_query_app())
    sidecar = Sidecar.from_dict({"by_stage": {"10": "query1", "11": "query9", "20": "query2"}})
    units = split_state(state, SPLIT_SQL, sidecar)
    assert units[0][0].query_id == "application_1_1#exec1"   # left unnamed, not guessed
    assert units[1][0].query_id == "query2"
    assert sidecar.conflicts and "query1" in sidecar.conflicts[0]


# --- tpcds_sidecar ---------------------------------------------------------


def _metrics_record(query_name, stage_id):
    return {"component": "spark", "data": {"query_name": query_name, "stage_id": stage_id}}


def _timer_record(query_name, nanos):
    return {"component": "spark",
            "data": {"query_name": query_name,
                     "execution_timer": {"query_name": query_name,
                                         "execution_time_in_ns": nanos}}}


def test_extract_sidecar_reads_a_metrics_directory(tmp_path):
    (tmp_path / "query1-1788700045.json").write_text(json.dumps(
        [_metrics_record("query1", 10), _metrics_record("query1", 11),
         _timer_record("query1", 20_649_845_070)]))
    (tmp_path / "query2-1788700099.json").write_text(
        json.dumps([_metrics_record("query2", 20)]))
    labels, exec_times, problems = extract_sidecar(tmp_path)
    assert labels == {10: "query1", 11: "query1", 20: "query2"}
    assert exec_times == {"query1": 20.649845}
    assert problems == []


def test_extract_sidecar_reads_a_tarball(tmp_path):
    src = tmp_path / "metrics"
    src.mkdir()
    (src / "query1-1788700045.json").write_text(json.dumps([_metrics_record("query1", 10)]))
    bundle = tmp_path / "run-metrics.tar.gz"
    with tarfile.open(bundle, "w:gz") as tf:
        tf.add(src, arcname="metrics")
    labels, exec_times, problems = extract_sidecar(bundle)
    assert labels == {10: "query1"}
    assert exec_times == {}
    assert problems == []


def test_extract_sidecar_drops_a_stage_two_queries_claim(tmp_path):
    (tmp_path / "query1-1.json").write_text(json.dumps([_metrics_record("query1", 10)]))
    (tmp_path / "query2-2.json").write_text(json.dumps([_metrics_record("query2", 10)]))
    labels, exec_times, problems = extract_sidecar(tmp_path)
    assert labels == {}
    assert problems and "stage 10" in problems[0]


def test_extract_sidecar_reports_disagreeing_timers(tmp_path):
    (tmp_path / "query1-1.json").write_text(json.dumps(
        [_metrics_record("query1", 10), _timer_record("query1", 1_000_000_000),
         _timer_record("query1", 2_000_000_000)]))
    labels, exec_times, problems = extract_sidecar(tmp_path)
    assert labels == {10: "query1"}
    assert exec_times == {}                       # not averaged, not picked
    assert problems and "disagreeing timer" in problems[0]


def test_extract_sidecar_reports_an_empty_bundle(tmp_path):
    labels, exec_times, problems = extract_sidecar(tmp_path)
    assert labels == {} and exec_times == {}
    assert problems and "no query*.json" in problems[0]


# --- the harness timing reaches the job row --------------------------------


def test_the_harness_timing_lands_on_the_named_unit():
    state = _run(_two_query_app())
    sidecar = Sidecar.from_dict({"by_stage": {"10": "query1", "11": "query1", "20": "query2"},
                                 "exec_time_s": {"query1": 4.5}})
    jobs = [build_results(sub, EnvConstants(), unit)[0]
            for unit, sub in split_state(state, SPLIT_SQL, sidecar)]
    assert jobs[0].harness_exec_time_s == 4.5
    assert jobs[1].harness_exec_time_s is None            # query2 has no timing
    # It is an alternative, never a replacement: exec_time_s keeps its own meaning.
    assert jobs[0].exec_time_s == pytest.approx(2.2)


def test_an_unnamed_unit_gets_no_timing():
    state = _run(_two_query_app())
    sidecar = Sidecar.from_dict({"exec_time_s": {"query1": 4.5}})
    jobs = [build_results(sub, EnvConstants(), unit)[0]
            for unit, sub in split_state(state, SPLIT_SQL, sidecar)]
    assert all(j.harness_exec_time_s is None for j in jobs)


# --- stage_duration_s, which IS the task span --------------------------------


def _task_end(stage_id, finish, run_ms, deserialize_ms=0):
    return {"Event": "SparkListenerTaskEnd", "Stage ID": stage_id,
            "Task Info": {"Finish Time": finish, "Accumulables": []},
            "Task Metrics": {"Executor Run Time": run_ms,
                             "Executor Deserialize Time": deserialize_ms}}


def test_stage_duration_reconstructs_each_task_start_from_its_busy_time():
    """The published definition: start := finish - (run time + deserialize time)."""
    events = [
        _app_start(ts=1_000_000), _executor_added(ts=1_000_100),
        _sql_start(1, 1_001_000), _stage_submitted(10, 1),
        # two tasks: one busy 1.0 s ending at +3.0, one busy 0.5 s ending at +4.0
        _task_end(10, 1_004_000, 1000), _task_end(10, 1_005_000, 500),
        _stage_completed(10, 1_001_000, 1_006_000),
        _sql_end(1, 1_006_500), _app_end(ts=1_007_000),
    ]
    (unit, sub), = split_state(_run(events), SPLIT_SQL)
    _, stages = build_results(sub, EnvConstants(), unit)
    # span = 1_005_000 - (1_004_000 - 1000) = 2.0 s
    #
    # Submission -> completion would be 5.0 s here. That reading is NOT emitted:
    # the extra 3 s is the stage pending for a slot, which the network does not
    # change and which double-counts across concurrent stages.
    assert stages[0].stage_duration_s == pytest.approx(2.0)


def test_stage_duration_is_zero_when_a_stage_reported_no_tasks():
    events = [
        _app_start(ts=1_000_000), _sql_start(1, 1_001_000), _stage_submitted(10, 1),
        _stage_completed(10, 1_001_000, 1_002_000),
        _sql_end(1, 1_002_500), _app_end(ts=1_003_000),
    ]
    (unit, sub), = split_state(_run(events), SPLIT_SQL)
    _, stages = build_results(sub, EnvConstants(), unit)
    assert stages[0].stage_duration_s == 0.0


def test_stage_duration_follows_its_stage_into_the_right_unit():
    events = list(_two_query_app())
    events.insert(-1, _task_end(20, 1_005_000, 400))
    events.insert(6, _task_end(10, 1_002_000, 300))
    units = split_state(_run(events), SPLIT_SQL)
    spans = {}
    for unit, sub in units:
        for st in build_results(sub, EnvConstants(), unit)[1]:
            spans[st.stage_id] = st.stage_duration_s
    # One task per stage, so each span is just that task's busy time - and, crucially,
    # each lands on its own unit rather than being pooled across the application.
    assert spans[10] == pytest.approx(0.3)
    assert spans[20] == pytest.approx(0.4)
    assert spans[11] == 0.0
