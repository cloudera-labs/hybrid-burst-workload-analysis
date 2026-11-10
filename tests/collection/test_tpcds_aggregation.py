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

"""Tests for the TPC-DS aggregation tables.

These tables used to come from throwaway scripts, which is why this module exists: a
number cited in a manuscript has to be reproducible by running committed code. The
tests pin the parts a silent change would corrupt, namely the configuration key the
runs are bucketed by, the join, the edge filter, and the arm definitions.
"""

from __future__ import annotations

import json

import pytest

from src.collection.analysis.tpcds_aggregation import (
    _CUMULATIVE,
    _DAG_PACKED,
    _DAG_PRINTED,
    _ONE_GROUP,
    _PUBLISHED,
    _PUBLISHED_PRINTED,
    _config,
    arm_predictions,
    load_runs,
    score,
)


# ─── Configuration key ───────────────────────────────────────────────────────


def test_config_reads_format_codec_bandwidth_and_latency():
    row = {"file_format_parquet": 1, "compression_codec_snappy": 1,
           "tsg_max_bw_gbs": 10, "network_latency_ms": 0.3}
    assert _config(row) == ("parquet", "snappy", 10.0, 0.3)


def test_config_falls_back_to_orc_and_uncompressed():
    row = {"file_format_parquet": 0, "tsg_max_bw_gbs": 2, "network_latency_ms": 25.46}
    assert _config(row) == ("orc", "uncompressed", 2.0, 25.46)


def test_config_separates_gzip_from_snappy():
    row = {"file_format_parquet": 1, "compression_codec_gzip": 1,
           "tsg_max_bw_gbs": 5, "network_latency_ms": 10.49}
    assert _config(row)[1] == "gzip"


# ─── Loading ─────────────────────────────────────────────────────────────────


def _write_dataset(tmp_path, stage_edges, stages=(0, 1), n_tasks=(1, 1),
                   durations=(4.0, 6.0), exec_time=10.0):
    cfg = {"file_format_parquet": 1, "compression_codec_snappy": 1,
           "tsg_max_bw_gbs": 10, "network_latency_ms": 0.3}
    (tmp_path / "enriched_measurement_summary.json").write_text(
        json.dumps([{**cfg, "query_id": "q1", "exec_time_s": exec_time}]))
    (tmp_path / "enriched_stage_measurement_summary.json").write_text(json.dumps([
        {**cfg, "query_id": "q1", "stage_id": sid, "stage_duration_s": d,
         "num_tasks": t, "input_read_gb": 1.0,
         "spark_num_executors": 2, "spark_executor_cores": 2}
        for sid, t, d in zip(stages, n_tasks, durations)
    ]))
    gdir = tmp_path / "graph" / "parquet_snappy"
    gdir.mkdir(parents=True)
    (gdir / "q1_groups.json").write_text(json.dumps({
        "stages": list(stages), "stage_edges": stage_edges,
        "groups": [{"members": list(stages)}], "scheduler_overhead_s": 0.0,
    }))
    return tmp_path


def test_load_runs_joins_jobs_stages_and_graphs(tmp_path):
    runs = load_runs(_write_dataset(tmp_path, []))
    assert len(runs) == 1
    assert runs[0]["E"] == 4
    assert runs[0]["observed"] == 10.0


def test_load_runs_drops_edges_naming_a_stage_with_no_row(tmp_path):
    """A graph can name a stage the summary lacks. Its edges must go too."""
    runs = load_runs(_write_dataset(tmp_path, [[0, 1], [1, 25]]))
    assert runs[0]["edges"] == [(0, 1)]


def test_load_runs_exits_when_the_summaries_are_missing(tmp_path):
    with pytest.raises(SystemExit):
        load_runs(tmp_path)


# ─── Arms ────────────────────────────────────────────────────────────────────


def test_cumulative_sums_every_stage(tmp_path):
    runs = load_runs(_write_dataset(tmp_path, []))
    assert arm_predictions(runs[0])[_CUMULATIVE] == pytest.approx(10.0)


def test_a_group_inside_the_core_budget_is_charged_its_longest_member(tmp_path):
    """Two stages, one task each, E = 4. They fit, so the group is max, not sum."""
    runs = load_runs(_write_dataset(tmp_path, []))
    assert arm_predictions(runs[0])[_ONE_GROUP] == pytest.approx(6.0)


def test_the_two_saturated_forms_differ_when_a_member_leaves_spare_slots(tmp_path):
    """E = 4. Stage 0: 10 s over 5 tasks. Stage 1: 3 s over 3 tasks.

    Packed pools 8 tasks into 2 waves of 4, charging 5 s each, so 10 s. The printed
    form charges both members in full, so 13 s.
    """
    runs = load_runs(_write_dataset(tmp_path, [], n_tasks=(5, 3), durations=(10.0, 3.0)))
    preds = arm_predictions(runs[0])
    assert preds[_PUBLISHED] == pytest.approx(10.0)
    assert preds[_PUBLISHED_PRINTED] == pytest.approx(13.0)


def test_dag_level_groups_split_a_dependency_the_published_group_merged(tmp_path):
    """With an edge 0 to 1 the level rule gives two groups, so the arm serialises."""
    runs = load_runs(_write_dataset(tmp_path, [[0, 1]]))
    preds = arm_predictions(runs[0])
    assert [g["members"] for g in runs[0]["dag"]] == [[0], [1]]
    assert preds[_DAG_PACKED] == pytest.approx(10.0)
    assert preds[_DAG_PRINTED] == pytest.approx(10.0)


def test_score_returns_a_metric_dict_per_arm(tmp_path):
    runs = load_runs(_write_dataset(tmp_path, []))
    scored = score(runs)
    assert set(scored) == {_CUMULATIVE, _PUBLISHED, _PUBLISHED_PRINTED,
                           _DAG_PACKED, _DAG_PRINTED, _ONE_GROUP}
    assert scored[_CUMULATIVE]["MAE"] == pytest.approx(0.0)
