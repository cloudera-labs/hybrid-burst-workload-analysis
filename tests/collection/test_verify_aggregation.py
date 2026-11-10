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

from src.collection.analysis.verify_aggregation import (
    _CUMULATIVE,
    _DAG_PACKED,
    _DAG_PAPER,
    _ONE_GROUP,
    _OBSERVED,
    _PACKED,
    _PRINTED,
    _cumulative_job_s,
    _group_time,
    _grouping_job_s,
    _resource_filling_time,
    compute_metrics,
    evaluate,
)


# --- group_time: the three branches -----------------------------------------


def test_sequential_group_is_its_own_time():
    feat = {5: {"time": 3.0, "num_tasks": 4}}
    assert _group_time([5], feat, E=8) == 3.0


def test_parallel_group_within_budget_is_the_max():
    # 2 + 2 = 4 tasks, budget 8 -> the group time is the longest member.
    feat = {0: {"time": 3.0, "num_tasks": 2}, 1: {"time": 5.0, "num_tasks": 2}}
    assert _group_time([0, 1], feat, E=8) == 5.0


def test_parallel_group_over_budget_is_wave_packed():
    # 8 + 8 = 16 tasks, budget 4 -> the tasks pack into waves, so it exceeds the max.
    feat = {0: {"time": 4.0, "num_tasks": 8}, 1: {"time": 4.0, "num_tasks": 8}}
    t = _group_time([0, 1], feat, E=4)
    assert t > 4.0
    # Each stage needs ceil(8/4)=2 waves, so tau = 4/2 = 2 per task; 16 tasks in
    # waves of 4 -> 4 waves of 2.0 = 8.0.
    assert t == 8.0


def test_saturated_group_printed_eq_is_the_sum_of_the_members():
    """Eq. (access.tex:514-517) as printed: the group is the sum of its members."""
    feat = {0: {"time": 10.0, "num_tasks": 5}, 1: {"time": 3.0, "num_tasks": 3}}
    assert _group_time([0, 1], feat, E=4, saturated=_PRINTED) == 13.0


def test_the_two_forms_coincide_when_every_member_fills_its_waves():
    """No spare slots to fill, so packing cannot recover anything."""
    feat = {0: {"time": 4.0, "num_tasks": 8}, 1: {"time": 4.0, "num_tasks": 8}}
    # tau = 4/2 = 2 for both; 16 tasks in waves of 4 -> 4 waves of 2.0.
    assert _group_time([0, 1], feat, E=4, saturated=_PRINTED) == 8.0
    assert _group_time([0, 1], feat, E=4, saturated=_PACKED) == 8.0


def test_the_forms_diverge_when_a_member_leaves_its_last_wave_partly_empty():
    """Stage 0 needs 2 waves for 5 tasks, so 3 slots are free for stage 1's tasks."""
    feat = {0: {"time": 10.0, "num_tasks": 5}, 1: {"time": 3.0, "num_tasks": 3}}
    # tau_0 = 10/2 = 5 (5 tasks), tau_1 = 3/1 = 3 (3 tasks).
    # Sorted desc [5,5,5,5,5,3,3,3]; 8 tasks in waves of 4 -> charge index 0 and 4.
    assert _group_time([0, 1], feat, E=4, saturated=_PACKED) == 10.0
    # The printed form charges stage 1 in full instead of hiding it behind stage 0.
    assert _group_time([0, 1], feat, E=4, saturated=_PRINTED) == 13.0


def test_the_two_forms_agree_outside_the_saturated_case():
    """A lone stage and a group inside the budget do not depend on the variant."""
    lone = {5: {"time": 3.0, "num_tasks": 4}}
    assert _group_time([5], lone, E=8, saturated=_PRINTED) == 3.0
    assert _group_time([5], lone, E=8, saturated=_PACKED) == 3.0

    fits = {0: {"time": 3.0, "num_tasks": 2}, 1: {"time": 5.0, "num_tasks": 2}}
    assert _group_time([0, 1], fits, E=8, saturated=_PRINTED) == 5.0
    assert _group_time([0, 1], fits, E=8, saturated=_PACKED) == 5.0


def test_resource_filling_matches_the_hand_computation():
    feat = {0: {"time": 6.0, "num_tasks": 6}}  # ceil(6/2)=3 waves, tau = 2.0
    # 6 tasks of 2.0 packed in waves of 2 -> 3 waves -> 6.0.
    assert _resource_filling_time([0], feat, E=2) == 6.0


# --- the two job arms --------------------------------------------------------


def test_cumulative_sums_every_stage_plus_overhead():
    feat = {0: {"time": 3.0, "num_tasks": 2}, 1: {"time": 5.0, "num_tasks": 2}}
    assert _cumulative_job_s(feat, overhead=2.0) == 10.0


def test_grouping_uses_overlap_then_adds_overhead():
    # One parallel group {0,1} (max=5) and one sequential group {2} (=4).
    feat = {
        0: {"time": 3.0, "num_tasks": 2},
        1: {"time": 5.0, "num_tasks": 2},
        2: {"time": 4.0, "num_tasks": 2},
    }
    groups = [{"members": [0, 1]}, {"members": [2]}]
    # 5 + 4 + overhead 2 = 11; the cumulative sum would be 3+5+4+2 = 14.
    assert _grouping_job_s(groups, feat, E=8, overhead=2.0) == 11.0


# --- end-to-end join over the three feature lists ----------------------------


def _feature_lists():
    job = {
        "query_id": "app_1",
        "exec_time_s": 11.0,
        "spark_num_executors": 2,
        "spark_executor_cores": 4,
    }
    stages = [
        {"query_id": "app_1", "stage_id": 0, "stage_duration_s": 3.0, "num_tasks": 2},
        {"query_id": "app_1", "stage_id": 1, "stage_duration_s": 5.0, "num_tasks": 2},
        {"query_id": "app_1", "stage_id": 2, "stage_duration_s": 4.0, "num_tasks": 2},
    ]
    graph = {
        "query_id": "app_1",
        "scheduler_overhead_s": 2.0,
        # Stage 2 depends on 0 and 1, so the DAG puts {0,1} on level 0 and {2} on
        # level 1. Here the overlap grouping happens to agree.
        "stage_edges": [[0, 2], [1, 2]],
        "dag_groups": [{"members": [0, 1]}, {"members": [2]}],
        "observed_groups": [{"members": [0, 1]}, {"members": [2]}],
    }
    return [job], stages, [graph]


def test_evaluate_joins_and_computes_every_arm():
    jobs, stages, graphs = _feature_lists()
    observed, preds, skipped = evaluate(jobs, stages, graphs)
    assert observed == [11.0]
    # 5 + 4 + 2, matches the observed time exactly. The group {0,1} fits the budget
    # of 8, so it is not saturated and both DAG arms agree here.
    assert preds[_DAG_PACKED] == [11.0]
    assert preds[_DAG_PAPER] == [11.0]
    assert preds[_OBSERVED] == [11.0]
    # 3 + 5 + 4 + 2, overcounts the parallel stages.
    assert preds[_CUMULATIVE] == [14.0]
    # One group of all three stages: 6 tasks fit the budget of 8, so max + 2.
    assert preds[_ONE_GROUP] == [7.0]
    assert skipped == 0


def test_evaluate_separates_the_two_dag_arms_when_a_group_saturates():
    """Give the group uneven task counts so the two Eq. forms diverge end to end."""
    jobs, stages, graphs = _feature_lists()
    jobs[0]["spark_num_executors"] = 2
    jobs[0]["spark_executor_cores"] = 2          # E = 4
    stages[0].update(stage_duration_s=10.0, num_tasks=5)   # stage 0
    stages[1].update(stage_duration_s=3.0, num_tasks=3)    # stage 1
    stages[2].update(stage_duration_s=4.0, num_tasks=2)    # stage 2, its own group

    observed, preds, skipped = evaluate(jobs, stages, graphs)
    # Group {0,1} saturates: 8 tasks against a budget of 4.
    #   printed -> 10 + 3 = 13, packed -> 5 + 5 = 10. Group {2} is 4. Overhead 2.
    assert preds[_DAG_PAPER] == [19.0]
    assert preds[_DAG_PACKED] == [16.0]
    assert skipped == 0


def test_evaluate_separates_the_dag_arm_from_the_observed_arm():
    """The two graphs disagree, so the two arms must give different numbers."""
    jobs, stages, graphs = _feature_lists()
    # The DAG still sees {0,1} then {2}, but nothing actually overlapped at runtime.
    graphs[0]["observed_groups"] = [{"members": [0]}, {"members": [1]}, {"members": [2]}]
    observed, preds, skipped = evaluate(jobs, stages, graphs)
    assert preds[_DAG_PACKED] == [11.0]   # 5 + 4 + 2
    assert preds[_OBSERVED] == [14.0]     # 3 + 5 + 4 + 2, every stage on its own
    assert skipped == 0


def test_evaluate_skips_a_job_missing_a_stage_record():
    jobs, stages, graphs = _feature_lists()
    stages = [s for s in stages if s["stage_id"] != 1]   # drop a group member
    observed, preds, skipped = evaluate(jobs, stages, graphs)
    assert observed == []
    assert skipped == 1


# --- metrics are the model's ------------------------------------------------


def test_perfect_reconstruction_scores_zero_error():
    m = compute_metrics([10.0, 20.0], [10.0, 20.0])
    assert m["MAE"] == 0.0
    assert m["RMSE"] == 0.0
    assert m["WMAPE%"] == 0.0
    assert m["R2"] == 1.0


def test_metrics_match_a_hand_computation():
    # observed 10 and 20, predicted 12 and 26: errors +2 and +6.
    m = compute_metrics([10.0, 20.0], [12.0, 26.0])
    assert round(m["MAE"], 6) == 4.0
    assert round(m["WMAPE%"], 6) == round(8.0 / 30.0 * 100.0, 6)
