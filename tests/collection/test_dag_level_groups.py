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

from src.collection.spark.model_parser import _dag_level_groups, _lift_groups


# --- the level rule ---------------------------------------------------------


def test_a_chain_puts_every_stage_on_its_own_level():
    groups = _dag_level_groups([0, 1, 2], [(0, 1), (1, 2)])
    assert groups == [[0], [1], [2]]


def test_independent_stages_share_level_zero():
    groups = _dag_level_groups([0, 1, 2], [])
    assert groups == [[0, 1, 2]]


def test_a_diamond_gives_three_levels():
    # 0 -> {1,2} -> 3
    groups = _dag_level_groups([0, 1, 2, 3], [(0, 1), (0, 2), (1, 3), (2, 3)])
    assert groups == [[0], [1, 2], [3]]


def test_level_is_the_longest_path_not_the_shortest():
    # 3 depends on 0 directly and on 0 through 1-2, so it must land on level 3,
    # never level 1. Otherwise a group would hold a stage and its own ancestor.
    groups = _dag_level_groups([0, 1, 2, 3], [(0, 1), (1, 2), (2, 3), (0, 3)])
    assert groups == [[0], [1], [2], [3]]


def test_every_group_is_an_antichain():
    stages = [0, 1, 2, 3, 4]
    edges = [(0, 2), (1, 2), (2, 3), (2, 4)]
    groups = _dag_level_groups(stages, edges)
    level_of = {sid: i for i, members in enumerate(groups) for sid in members}
    for parent, child in edges:
        assert level_of[parent] < level_of[child]


def test_a_stage_with_no_edges_stays_on_level_zero():
    groups = _dag_level_groups([0, 1, 2], [(0, 1)])
    assert groups == [[0, 2], [1]]


# --- lifting the partition into group records -------------------------------


def test_lift_builds_edges_kinds_and_the_stage_map():
    groups, group_edges, stage_to_group = _lift_groups([[0, 1], [2]], [(0, 2), (1, 2)])
    assert stage_to_group == {0: 0, 1: 0, 2: 1}
    assert group_edges == [[0, 1]]          # both stage edges collapse to one
    assert groups[0]["kind"] == "parallel"
    assert groups[1]["kind"] == "sequential"
    assert groups[1]["parents"] == [0]


def test_lift_drops_an_edge_inside_one_group():
    _, group_edges, _ = _lift_groups([[0, 1]], [(0, 1)])
    assert group_edges == []
