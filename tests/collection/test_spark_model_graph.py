import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.spark.model_parser import (
    _State,
    build_graph,
    build_results,
    process_event,
)

# Timestamps are epoch milliseconds, as in a real event log.


# --- event builders --------------------------------------------------------


def _app_start(app_id="application_1_1", name="app", ts=1_000):
    return {"Event": "SparkListenerApplicationStart", "App ID": app_id,
            "App Name": name, "Timestamp": ts}


def _app_end(ts=11_000):
    return {"Event": "SparkListenerApplicationEnd", "Timestamp": ts}


def _executor_added(exec_id="1", ts=1_500, cores=4):
    return {"Event": "SparkListenerExecutorAdded", "Executor ID": exec_id,
            "Timestamp": ts, "Executor Info": {"Total Cores": cores}}


def _task_start(stage_id, launch):
    return {"Event": "SparkListenerTaskStart", "Stage ID": stage_id,
            "Task Info": {"Launch Time": launch}}


def _stage_completed(stage_id, submission, completion, num_tasks=4, parent_ids=None):
    info = {"Stage ID": stage_id, "Number of Tasks": num_tasks,
            "Submission Time": submission, "Completion Time": completion,
            "Parent IDs": parent_ids or [], "Accumulables": []}
    return {"Event": "SparkListenerStageCompleted", "Stage Info": info}


def _run(events):
    state = _State()
    for e in events:
        process_event(e, state)
    return state


# A canonical run: stages 0 and 1 overlap in wall-clock (one parallel group); stage 2
# is a child of stage 1 that runs after both (its own sequential group).
def _canonical_events():
    return [
        _app_start(ts=1_000),
        _executor_added(ts=1_500),
        _task_start(0, 2_500),                 # stage 0 waited 500 ms for a slot
        _task_start(1, 3_000),                 # stage 1 started at submission (no wait)
        _stage_completed(0, submission=2_000, completion=5_000),
        _stage_completed(1, submission=3_000, completion=6_000),
        _stage_completed(2, submission=7_000, completion=9_000, parent_ids=[1]),
        _app_end(ts=11_000),
    ]


# --- stage DAG & grouping --------------------------------------------------


def test_stage_edges_from_parent_ids():
    g = build_graph(_run(_canonical_events()))
    assert g.stages == [0, 1, 2]
    assert g.stage_edges == [[1, 2]]


def test_time_overlap_forms_one_parallel_group():
    g = build_graph(_run(_canonical_events()))
    by_gid = {grp["gid"]: grp for grp in g.groups}
    # Stages 0 and 1 overlap in time -> one parallel group.
    assert by_gid[0]["members"] == [0, 1]
    assert by_gid[0]["kind"] == "parallel"
    # Stage 2 runs in a later, disjoint window -> its own sequential group.
    assert by_gid[1]["members"] == [2]
    assert by_gid[1]["kind"] == "sequential"


def test_group_edges_and_stage_to_group():
    g = build_graph(_run(_canonical_events()))
    assert g.stage_to_group == {0: 0, 1: 0, 2: 1}
    # stage_edge (1,2) lifts to (group 0 -> group 1).
    assert g.group_edges == [[0, 1]]
    assert {grp["gid"]: grp["parents"] for grp in g.groups} == {0: [], 1: [0]}


def test_disjoint_waves_split_without_edges():
    # Two concurrent pairs with a time gap and NO dependency between them -> two
    # separate parallel groups (the query33 shape).
    events = [
        _app_start(ts=1_000),
        _stage_completed(0, 2_000, 4_000),
        _stage_completed(1, 2_500, 4_500),     # overlaps 0
        _stage_completed(2, 8_000, 9_000),
        _stage_completed(3, 8_200, 9_200),     # overlaps 2, disjoint from 0/1
        _app_end(ts=11_000),
    ]
    g = build_graph(_run(events))
    members = sorted((grp["members"] for grp in g.groups), key=lambda m: m[0])
    assert members == [[0, 1], [2, 3]]
    assert all(grp["kind"] == "parallel" for grp in g.groups)
    assert g.group_edges == []                 # no edges between the waves


def test_transitive_overlap_still_keeps_groups_antichain():
    # A concurrent wave where everything overlaps in time, but it contains dependency
    # pairs (0->1, 2->3). Pure connected-components would merge all four; the antichain
    # split must keep no DAG-comparable pair together, so 0/1 and 2/3 land apart.
    events = [
        _app_start(ts=1_000),
        _stage_completed(0, 2_000, 6_000),
        _stage_completed(1, 2_100, 6_100, parent_ids=[0]),   # child of 0, overlaps all
        _stage_completed(2, 2_200, 6_200),
        _stage_completed(3, 2_300, 6_300, parent_ids=[2]),   # child of 2, overlaps all
        _app_end(ts=8_000),
    ]
    g = build_graph(_run(events))
    s2g = g.stage_to_group
    assert s2g[0] != s2g[1]                       # parent/child never co-grouped
    assert s2g[2] != s2g[3]
    # No stage_edge sits inside a group (the model's invariant).
    assert all(s2g[u] != s2g[v] for u, v in g.stage_edges)


def test_dag_linked_stages_never_cogrouped_even_if_overlapping():
    # Defensive guard: a parent/child pair whose intervals overlap must still land in
    # separate groups (the DAG edge forbids co-grouping).
    events = [
        _app_start(ts=1_000),
        _stage_completed(0, 2_000, 6_000),
        _stage_completed(1, 3_000, 5_000, parent_ids=[0]),   # overlaps 0 but is its child
        _app_end(ts=8_000),
    ]
    g = build_graph(_run(events))
    assert g.stage_to_group[0] != g.stage_to_group[1]
    assert g.stage_edges == [[0, 1]]
    assert g.group_edges == [[g.stage_to_group[0], g.stage_to_group[1]]]


# --- time decomposition ----------------------------------------------------


def test_scheduler_overhead_is_planning_startup_only():
    g = build_graph(_run(_canonical_events()))
    # wall=10s, active = 9000-2000 = 7s, resource_wait = (1500-1000)/1000 = 0.5s,
    # planning = wall - active - resource_wait = 2.5s.
    assert g.scheduler_overhead_s == 2.5


def test_job_time_fields_partition_wall_clock():
    job, _ = build_results(_run(_canonical_events()))
    assert job.wall_clock_s == 10.0
    assert job.resource_wait_s == 0.5
    assert job.planning_startup_s == 2.5
    # exec_time_s = wall - resource_wait = active + planning (resource wait excluded).
    assert job.exec_time_s == 9.5
    # The parts partition wall-clock exactly.
    assert round(job.resource_wait_s + job.planning_startup_s
                 + (job.exec_time_s - job.planning_startup_s), 6) == job.wall_clock_s


def test_stage_duration_excludes_the_wait_for_a_slot():
    """The stage clock is the task span, so pending-for-a-slot is not in it.

    Stage 0 is submitted at 2_000 and completes at 5_000 - three seconds - but
    its one task launches at 2_500 and is busy for one second. Submission ->
    completion would call that 3.0 s and count the 500 ms it spent waiting for a
    slot, plus the 1.5 s after its task finished. The span is 1.0 s.

    That matters because the network changes how long a task takes and not how
    long a stage waits for a slot, and because sibling stages wait concurrently,
    so the excluded part double-counts when summed.
    """
    events = list(_canonical_events())
    events.insert(4, {"Event": "SparkListenerTaskEnd", "Stage ID": 0,
                      "Task Info": {"Finish Time": 3_500, "Accumulables": []},
                      "Task Metrics": {"Executor Run Time": 1_000,
                                       "Executor Deserialize Time": 0}})
    _, stages = build_results(_run(events))
    by_id = {s.stage_id: s for s in stages}
    assert by_id[0].stage_duration_s == pytest.approx(1.0)
    # A stage that reported no task has no task time to span.
    assert by_id[2].stage_duration_s == 0.0
    # The stage-level wait column is gone; the JOB-level one is the real thing.
    assert not hasattr(by_id[0], "resource_wait_s")
    assert not hasattr(by_id[0], "stage_task_span_s")


def test_no_resource_wait_without_executor_added():
    # No ExecutorAdded -> we do not claim any resource wait (0), so planning absorbs the
    # whole non-active overhead.
    events = [
        _app_start(ts=1_000),
        _stage_completed(0, 2_000, 5_000),
        _app_end(ts=11_000),
    ]
    job, _ = build_results(_run(events))
    assert job.resource_wait_s == 0.0
    assert job.wall_clock_s == 10.0
    assert job.exec_time_s == 10.0                # nothing excluded


# --- edges cases -----------------------------------------------------------


def test_build_graph_none_without_app_start():
    assert build_graph(_run([_stage_completed(0, 1, 2), _app_end()])) is None


def test_query_id_joins_to_job_and_stage():
    events = _canonical_events()
    state = _run(events)
    g = build_graph(state)
    job, stages = build_results(state)
    assert g.query_id == job.query_id == g.app_id == "application_1_1"
    assert {s.query_id for s in stages} == {g.query_id}
