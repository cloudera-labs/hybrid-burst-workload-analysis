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

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.analysis.stage_matcher import (
    DEFAULT_MIN_SHAPE,
    match_runs,
    match_stages,
    report,
    shape_score,
)


def stage(stage_id, tasks, input_gb=0.0, sread=0.0, swrite=0.0, duration=1.0,
          query="query1", cpu=1.0):
    return {"query_id": query, "stage_id": stage_id, "unique_stage_id": stage_id,
            "num_tasks": tasks, "input_read_gb": input_gb, "shuffle_read_gb": sread,
            "shuffle_write_gb": swrite, "stage_duration_s": duration,
            "executor_cpu_time_s": cpu}


# --- the shape score -------------------------------------------------------


def test_identical_stages_score_full_marks():
    a = stage(0, 100, input_gb=5.0, sread=1.0, swrite=2.0)
    assert shape_score(a, a) == pytest.approx(10.0)


def test_duration_never_influences_the_score():
    """Duration is what the comparison exists to measure; it must not pick the pair."""
    a = stage(0, 100, input_gb=5.0, duration=1.0)
    b = stage(0, 100, input_gb=5.0, duration=900.0)
    assert shape_score(a, b) == pytest.approx(10.0)


def test_a_missing_field_is_skipped_not_penalised():
    """The released raw stage dataset has no per-stage shuffle. Score what exists."""
    full = stage(0, 100, input_gb=5.0, sread=3.0, swrite=4.0)
    thin = {"num_tasks": 100, "input_read_gb": 5.0}
    assert shape_score(thin, thin) == pytest.approx(10.0)
    # and a thin row still scores against a full one on the fields they share
    assert shape_score(thin, full) == pytest.approx(10.0)


def test_two_zeros_carry_no_evidence_and_are_skipped():
    """Most stages read no input, so agreeing on that says nothing about identity.

    Scoring it as agreement let an 11-task stage pair with a 7-task one at 7.3/10.
    Skipped, the same pair scores below the floor and is left to the fallback.
    """
    a = stage(0, 10, input_gb=0.0)
    b = stage(3, 10, input_gb=0.0)
    assert shape_score(a, b) == pytest.approx(10.0)      # equal task counts still carry it

    near = shape_score(stage(0, 11, input_gb=0.0), stage(0, 7, input_gb=0.0))
    assert near < DEFAULT_MIN_SHAPE, "a 36% task-count gap must not pass on two zeros"


def test_one_zero_against_a_real_value_is_a_disagreement():
    """Reading input and not reading input is a real difference, not a missing field."""
    assert shape_score(stage(0, 100, input_gb=0.0),
                       stage(0, 100, input_gb=5.0)) < DEFAULT_MIN_SHAPE


def test_a_different_shape_scores_lower():
    a = stage(0, 100, input_gb=10.0)
    b = stage(0, 1, input_gb=0.0)
    assert shape_score(a, b) < DEFAULT_MIN_SHAPE


# --- structural guarantees of the alignment --------------------------------


def _left_right():
    left = [stage(0, 100, input_gb=10.0), stage(1, 50, input_gb=5.0),
            stage(2, 10, sread=1.0), stage(3, 5, sread=0.5)]
    right = [stage(0, 100, input_gb=10.0), stage(1, 50, input_gb=5.0),
             stage(2, 10, sread=1.0), stage(3, 5, sread=0.5)]
    return left, right


def test_every_stage_gets_a_match_nothing_is_dropped():
    left, right = _left_right()
    right.append(stage(4, 999, input_gb=77.0))          # no counterpart at all
    matches = match_stages(left, right)
    assert len(matches) == len(right)
    assert all(m.left_index is not None for m in matches)
    assert matches[-1].kind == "fallback"


def test_the_aligned_tier_is_one_to_one():
    left, right = _left_right()
    right.append(stage(4, 100, input_gb=10.0))          # a second twin of left[0]
    aligned = [m for m in match_stages(left, right) if m.kind == "aligned"]
    used = [m.left_index for m in aligned]
    assert len(used) == len(set(used)), "a baseline stage was reused inside the aligned tier"


def test_the_aligned_tier_preserves_order():
    left, right = _left_right()
    aligned = [m for m in match_stages(left, right) if m.kind == "aligned"]
    seq = [m.left_index for m in aligned]
    assert seq == sorted(seq), "the alignment inverted the stage order"


def test_reuse_is_confined_to_the_fallback_pass():
    left = [stage(0, 100, input_gb=10.0)]
    right = [stage(0, 100, input_gb=10.0), stage(1, 100, input_gb=10.0)]
    matches = match_stages(left, right)
    assert [m.kind for m in matches] == ["aligned", "fallback"]
    assert matches[0].left_index == matches[1].left_index == 0


def test_an_empty_reference_run_yields_unmatched_not_a_crash():
    matches = match_stages([], [stage(0, 10)])
    assert [m.kind for m in matches] == ["unmatched"]
    assert matches[0].left_index is None


# --- position is a tie-break, never a term ---------------------------------


def test_position_separates_structurally_identical_stages():
    """Four indistinguishable one-task stages: only position can order them."""
    left = [stage(i, 1) for i in range(4)]
    right = [stage(i, 1) for i in range(4)]
    matches = match_stages(left, right)
    assert [m.left_index for m in matches] == [0, 1, 2, 3]


def test_position_cannot_outrank_a_better_shape():
    """The right partner sits in the wrong place; shape must still win."""
    left = [stage(0, 1, input_gb=0.0), stage(1, 500, input_gb=50.0)]
    right = [stage(0, 500, input_gb=50.0)]
    matches = match_stages(left, right)
    assert matches[0].left_index == 1
    assert matches[0].kind == "aligned"


def test_the_tie_break_is_excluded_from_the_reported_quality():
    """quality says how alike the stages are, not how the tie was broken."""
    left = [stage(0, 1)]
    right = [stage(0, 1)]
    heavy = match_stages(left, right, ordinal_weight=5.0)[0]
    none_ = match_stages(left, right, ordinal_weight=0.0)[0]
    assert heavy.quality == none_.quality == pytest.approx(10.0)


def test_ordinal_weight_zero_turns_position_off():
    """Across runtimes position stops being evidence; the caller can disable it."""
    left = [stage(i, 1) for i in range(3)]
    right = [stage(i, 1) for i in range(3)]
    off = match_stages(left, right, ordinal_weight=0.0)
    # still one-to-one and ordered, just no longer steered by position
    seq = [m.left_index for m in off if m.kind == "aligned"]
    assert seq == sorted(seq)
    assert len(seq) == len(set(seq))


# --- quality tiers ---------------------------------------------------------


def test_is_good_requires_alignment_and_a_real_shape_match():
    left = [stage(0, 100, input_gb=10.0)]
    right = [stage(0, 100, input_gb=10.0), stage(1, 100, input_gb=10.0)]
    aligned, fallback = match_stages(left, right)
    assert aligned.is_good
    assert not fallback.is_good, "a fallback must never pass as calibration-grade"


def test_a_poor_pair_is_reported_as_poor_rather_than_hidden():
    left = [stage(0, 1, input_gb=0.0)]
    right = [stage(0, 900, input_gb=90.0)]
    match, = match_stages(left, right)
    assert match.left_index == 0          # still matched
    assert match.quality < DEFAULT_MIN_SHAPE
    assert not match.is_good


# --- whole runs ------------------------------------------------------------


def test_match_runs_never_pairs_across_queries():
    left = [stage(0, 100, input_gb=10.0, query="query1"),
            stage(0, 100, input_gb=10.0, query="query2")]
    right = [stage(0, 100, input_gb=10.0, query="query2")]
    (row, partner, match), = match_runs(left, right)
    assert row["query_id"] == partner["query_id"] == "query2"


def test_match_runs_covers_a_query_absent_from_the_reference():
    left = [stage(0, 100, input_gb=10.0, query="query1")]
    right = [stage(0, 100, input_gb=10.0, query="query9")]
    (row, partner, match), = match_runs(left, right)
    assert partner is None and match.kind == "unmatched"


def test_the_report_splits_the_tiers():
    left = [stage(0, 100, input_gb=10.0), stage(1, 20, sread=2.0)]
    right = [stage(0, 100, input_gb=10.0), stage(1, 20, sread=2.0)]
    lines = report(match_runs(left, right))
    assert any(line.startswith("transfer") for line in lines)
    assert any(line.startswith("no-transfer") for line in lines)
