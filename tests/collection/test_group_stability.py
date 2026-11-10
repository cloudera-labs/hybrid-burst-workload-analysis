"""Tests for the group-stability gate.

The gate asks whether an earlier run's overlap grouping can stand in for the one
read from the run being scored. These tests pin the parts that decide whether the
answer means anything: run ordering, the transfer rule, the antichain check that
says when a carried-over grouping is inconsistent with the recipient's DAG, and the
family key that keeps plan changes out of the comparison.
"""

from __future__ import annotations

import pytest

from src.collection.analysis.group_stability import (
    _ARMS,
    _DAG,
    _OWN,
    _TRANSFER,
    _app_order,
    _breaks_antichain,
    _descendants,
    _transfer,
    build_runs,
    evaluate,
    families,
)


# ─── Run ordering ────────────────────────────────────────────────────────────


def test_app_order_sorts_by_sequence_number_not_lexically():
    """String order puts _100 before _99. Submission order does not."""
    ids = ["application_1778998205867_99", "application_1778998205867_100"]
    assert sorted(ids, key=_app_order) == ids
    assert sorted(ids) != ids


def test_app_order_separates_cluster_epochs():
    early = _app_order("application_1000000000000_500")
    late = _app_order("application_2000000000000_1")
    assert early < late


def test_app_order_is_total_on_unparseable_ids():
    assert _app_order("not-an-app-id") == (0, 0)


# ─── Transfer ────────────────────────────────────────────────────────────────


def test_transfer_carries_the_donor_partition_over():
    donor = [{"members": [0, 1]}, {"members": [2]}]
    assert _transfer(donor, {0, 1, 2}) == [{"members": [0, 1]}, {"members": [2]}]


def test_transfer_drops_donor_stages_the_recipient_does_not_have():
    donor = [{"members": [0, 1, 7]}, {"members": [9]}]
    # Stage 9 is gone entirely, so its group disappears rather than becoming empty.
    assert _transfer(donor, {0, 1}) == [{"members": [0, 1]}]


def test_transfer_makes_an_unseen_recipient_stage_its_own_group():
    """The conservative choice: charge it in full instead of hiding it in a group."""
    donor = [{"members": [0, 1]}]
    assert _transfer(donor, {0, 1, 2}) == [{"members": [0, 1]}, {"members": [2]}]


def test_transfer_covers_every_recipient_stage_exactly_once():
    donor = [{"members": [0, 5]}, {"members": [1]}]
    groups = _transfer(donor, {0, 1, 2, 3})
    covered = [sid for g in groups for sid in g["members"]]
    assert sorted(covered) == [0, 1, 2, 3]
    assert len(covered) == len(set(covered))


# ─── Antichain invariant ─────────────────────────────────────────────────────


def test_descendants_are_transitive():
    desc = _descendants({0, 1, 2}, [[0, 1], [1, 2]])
    assert desc[0] == {1, 2}
    assert desc[2] == set()


def test_antichain_holds_for_independent_stages():
    desc = _descendants({0, 1}, [])
    assert not _breaks_antichain([{"members": [0, 1]}], desc)


def test_antichain_breaks_when_a_group_holds_a_stage_and_its_descendant():
    """A carried-over grouping can assert that dependent stages ran together."""
    desc = _descendants({0, 1, 2}, [[0, 1], [1, 2]])
    assert _breaks_antichain([{"members": [0, 2]}], desc)


# ─── Families ────────────────────────────────────────────────────────────────


def _run(app_id, name, stage_ids, seq_time=1.0):
    feat = {sid: {"time": seq_time, "num_tasks": 1} for sid in stage_ids}
    return {
        "app_id": app_id,
        "name": name,
        "order": _app_order(app_id),
        "feat": feat,
        "stage_ids": set(stage_ids),
        "edges": [],
        "dag_groups": [{"members": sorted(stage_ids)}],
        "obs_groups": [{"members": [sid]} for sid in sorted(stage_ids)],
        "E": 4,
        "overhead": 0.0,
        "observed": seq_time * len(stage_ids),
    }


def test_families_needs_at_least_two_runs():
    runs = [_run("application_1_1", "etl", [0, 1])]
    assert families(runs) == {}


def test_families_separate_on_stage_count_so_plan_changes_do_not_mix():
    """Same name, different stage count, is a different plan and a different family."""
    runs = [
        _run("application_1_1", "etl", [0, 1]),
        _run("application_1_2", "etl", [0, 1]),
        _run("application_1_3", "etl", [0, 1, 2]),
    ]
    fams = families(runs)
    assert set(fams) == {("etl", 2)}
    assert len(fams[("etl", 2)]) == 2


def test_families_are_ordered_in_time():
    runs = [
        _run("application_1_20", "etl", [0, 1]),
        _run("application_1_3", "etl", [0, 1]),
    ]
    order = [r["app_id"] for r in families(runs)[("etl", 2)]]
    assert order == ["application_1_3", "application_1_20"]


# ─── Evaluate ────────────────────────────────────────────────────────────────


def test_evaluate_never_scores_a_run_with_its_own_groups():
    """The donor must be a different execution, or the gate proves nothing."""
    runs = [
        _run("application_1_1", "etl", [0, 1]),
        _run("application_1_2", "etl", [0, 1]),
    ]
    result = evaluate(runs, "previous")
    # Two runs in the family means exactly one scored pair: the second run.
    assert len(result["observed"]) == 1


def test_evaluate_scores_every_arm_on_the_same_runs():
    runs = [
        _run("application_1_1", "etl", [0, 1]),
        _run("application_1_2", "etl", [0, 1]),
        _run("application_1_3", "etl", [0, 1]),
    ]
    result = evaluate(runs, "previous")
    lengths = {len(result["preds"][a]) for a in _ARMS}
    assert lengths == {len(result["observed"])} == {2}


def test_transferred_matches_own_when_the_structure_is_identical():
    """The floor case: if two runs group the same way, transfer costs nothing."""
    runs = [
        _run("application_1_1", "etl", [0, 1, 2]),
        _run("application_1_2", "etl", [0, 1, 2]),
    ]
    result = evaluate(runs, "previous")
    assert result["preds"][_TRANSFER] == result["preds"][_OWN]
    assert result["antichain_broken"] == 0


def test_transferred_differs_from_dag_when_the_dag_merges_what_ran_in_sequence():
    """The case the production trace is full of: no edges, so the DAG sees one group.

    Three stages of 6 s each, 1 task apiece, E = 4. The DAG puts all three on level 0
    and packs them into one wave, charging 6 s. The overlap grouping saw them run one
    after another, so it charges 18 s, which is what actually happened.
    """
    runs = [
        _run("application_1_1", "etl", [0, 1, 2], seq_time=6.0),
        _run("application_1_2", "etl", [0, 1, 2], seq_time=6.0),
    ]
    result = evaluate(runs, "previous")
    assert result["preds"][_DAG] == [6.0]
    assert result["preds"][_TRANSFER] == [18.0]
    assert result["observed"] == [18.0]


def test_first_policy_holds_the_donor_fixed():
    runs = [_run(f"application_1_{i}", "etl", [0, 1]) for i in range(1, 5)]
    assert len(evaluate(runs, "first")["observed"]) == 3


def test_build_runs_skips_a_graph_naming_a_stage_with_no_stage_record():
    jobs = [{
        "app_id": "application_1_1", "app_name": "etl", "query_id": "q1",
        "exec_time_s": 10.0, "spark_num_executors": 2, "spark_executor_cores": 2,
    }]
    stages = [{"query_id": "q1", "stage_id": 0, "stage_duration_s": 10.0, "num_tasks": 1}]
    graphs = [{
        "query_id": "q1", "name": "etl", "stages": [0, 1], "stage_edges": [],
        "dag_groups": [{"members": [0, 1]}],
        "observed_groups": [{"members": [0]}, {"members": [1]}],
        "scheduler_overhead_s": 0.0,
    }]
    assert build_runs(jobs, stages, graphs) == []


def test_build_runs_defaults_a_missing_core_budget_to_one():
    jobs = [{
        "app_id": "application_1_1", "app_name": "etl", "query_id": "q1",
        "exec_time_s": 10.0, "spark_num_executors": None, "spark_executor_cores": None,
    }]
    stages = [{"query_id": "q1", "stage_id": 0, "stage_duration_s": 10.0, "num_tasks": 1}]
    graphs = [{
        "query_id": "q1", "name": "etl", "stages": [0], "stage_edges": [],
        "dag_groups": [{"members": [0]}],
        "observed_groups": [{"members": [0]}],
        "scheduler_overhead_s": 0.0,
    }]
    assert build_runs(jobs, stages, graphs)[0]["E"] == 1


def test_build_runs_exits_when_the_dag_groups_are_missing():
    jobs = [{
        "app_id": "application_1_1", "app_name": "etl", "query_id": "q1",
        "exec_time_s": 10.0, "spark_num_executors": 1, "spark_executor_cores": 1,
    }]
    stages = [{"query_id": "q1", "stage_id": 0, "stage_duration_s": 10.0, "num_tasks": 1}]
    graphs = [{"query_id": "q1", "name": "etl", "stages": [0], "stage_edges": [],
               "observed_groups": [{"members": [0]}], "scheduler_overhead_s": 0.0}]
    with pytest.raises(SystemExit):
        build_runs(jobs, stages, graphs)


# ─── Per-run rows, which the subset tables are built from ────────────────────


def test_partition_ignores_group_order_and_member_order():
    from src.collection.analysis.group_stability import _partition

    assert _partition([{"members": [1, 0]}, {"members": [2]}]) == _partition(
        [{"members": [2]}, {"members": [0, 1]}]
    )


def test_rows_flag_when_the_transferred_partition_matches_the_recipients_own():
    runs = [
        _run("application_1_1", "etl", [0, 1]),
        _run("application_1_2", "etl", [0, 1]),
    ]
    rows = evaluate(runs, "previous")["rows"]
    assert [r["identical"] for r in rows] == [True]


def test_rows_carry_the_shape_fields_the_subset_tables_group_on():
    runs = [
        _run("application_1_1", "etl", [0, 1, 2]),
        _run("application_1_2", "etl", [0, 1, 2]),
    ]
    row = evaluate(runs, "previous")["rows"][0]
    assert row["n_stages"] == 3
    # The fixture's overlap grouping is all singletons, so nothing is multi-stage.
    assert row["n_multi"] == 0
    assert row["n_obs_groups"] == 3
    assert row["n_dag_groups"] == 1


def test_rows_and_preds_stay_aligned():
    runs = [_run(f"application_1_{i}", "etl", [0, 1]) for i in range(1, 5)]
    result = evaluate(runs, "previous")
    assert [r[_DAG] for r in result["rows"]] == result["preds"][_DAG]
    assert [r["observed"] for r in result["rows"]] == result["observed"]
