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

"""Tests for the error decomposition.

The module answers which layer produces job-level error. Its whole credibility rests
on the decomposition being an identity rather than a model, and on the two stage-time
sources being exactly what they claim. Those are what is pinned here.

Note what is deliberately absent: there is no scaling parameter to test. An earlier
version scaled the observed stage times and read a "sensitivity threshold" off the
scale at which the ranking changed. That was circular, because each arm carries its
own job-level bias and deflating the input just orders the arms by that bias. The
test that would have caught it is `test_scaling_input_recovers_the_bias_ordering`
below, kept as a regression guard against reintroducing the design.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from src.collection.analysis.error_sensitivity import (
    _ARMS,
    _CUMULATIVE,
    _DAG_PACKED,
    _PUBLISHED,
    _feat_with,
    _predict,
    _shape,
    both_predictions,
    stage_error_signature,
)
from src.collection.analysis.tpcds_aggregation import load_runs


def _dataset(tmp_path, durations=(4.0, 6.0), preds=(2.0, 3.0), n_tasks=(1, 1),
             exec_time=10.0):
    cfg = {"file_format_parquet": 1, "compression_codec_snappy": 1,
           "tsg_max_bw_gbs": 10, "network_latency_ms": 0.3}
    (tmp_path / "enriched_measurement_summary.json").write_text(
        json.dumps([{**cfg, "query_id": "q1", "exec_time_s": exec_time}]))
    (tmp_path / "enriched_stage_measurement_summary.json").write_text(json.dumps([
        {**cfg, "query_id": "q1", "stage_id": i, "stage_duration_s": d,
         "T_pred_total": pr, "num_tasks": t, "input_read_gb": 1.0,
         "spark_num_executors": 2, "spark_executor_cores": 2}
        for i, (d, pr, t) in enumerate(zip(durations, preds, n_tasks))
    ]))
    gdir = tmp_path / "graph" / "parquet_snappy"
    gdir.mkdir(parents=True)
    (gdir / "q1_groups.json").write_text(json.dumps({
        "stages": list(range(len(durations))), "stage_edges": [],
        "groups": [{"members": list(range(len(durations)))}],
        "scheduler_overhead_s": 0.0,
    }))
    return tmp_path


# ─── The two stage-time sources ──────────────────────────────────────────────


def test_observed_source_uses_the_measured_durations(tmp_path):
    run = load_runs(_dataset(tmp_path))[0]
    assert [f["time"] for f in _feat_with(run, "observed").values()] == [4.0, 6.0]


def test_predicted_source_uses_the_models_stage_times(tmp_path):
    run = load_runs(_dataset(tmp_path))[0]
    assert [f["time"] for f in _feat_with(run, "predicted").values()] == [2.0, 3.0]


def test_task_counts_come_from_the_run_not_the_source(tmp_path):
    run = load_runs(_dataset(tmp_path, n_tasks=(5, 3)))[0]
    for source in ("observed", "predicted"):
        assert [f["num_tasks"] for f in _feat_with(run, source).values()] == [5, 3]


def test_a_run_with_no_prediction_is_dropped_from_both_sides(tmp_path):
    """The two sides must be scored on exactly the same runs."""
    d = _dataset(tmp_path)
    rows = json.loads((d / "enriched_stage_measurement_summary.json").read_text())
    rows[0]["T_pred_total"] = None
    (d / "enriched_stage_measurement_summary.json").write_text(json.dumps(rows))
    kept, observed, on_obs, on_pred = both_predictions(load_runs(d))
    assert kept == [] and len(observed) == 0
    assert all(len(on_obs[a]) == 0 and len(on_pred[a]) == 0 for a in _ARMS)


# ─── The decomposition is an identity, not a model ───────────────────────────


def test_decomposition_is_exact(tmp_path):
    """e_total = e_agg + e_prop must hold to floating point, for every arm."""
    runs = load_runs(_dataset(tmp_path, n_tasks=(5, 3), durations=(10.0, 3.0),
                              preds=(8.0, 2.0)))
    _, observed, on_obs, on_pred = both_predictions(runs)
    for a in _ARMS:
        e_agg = on_obs[a] - observed
        e_prop = on_pred[a] - on_obs[a]
        assert np.allclose(on_pred[a] - observed, e_agg + e_prop)


def test_aggregation_error_is_zero_when_the_aggregation_is_exact(tmp_path):
    """Two sequential stages of 4 s and 6 s against a 10 s job: cumulative is right."""
    runs = load_runs(_dataset(tmp_path, exec_time=10.0))
    _, observed, on_obs, _ = both_predictions(runs)
    assert (on_obs[_CUMULATIVE] - observed)[0] == pytest.approx(0.0)


def test_propagated_error_is_zero_when_the_stage_model_is_exact(tmp_path):
    runs = load_runs(_dataset(tmp_path, preds=(4.0, 6.0)))
    _, _, on_obs, on_pred = both_predictions(runs)
    for a in _ARMS:
        assert (on_pred[a] - on_obs[a])[0] == pytest.approx(0.0)


def test_under_predicting_stage_times_propagate_as_negative_error(tmp_path):
    runs = load_runs(_dataset(tmp_path))
    _, _, on_obs, on_pred = both_predictions(runs)
    assert (on_pred[_CUMULATIVE] - on_obs[_CUMULATIVE])[0] < 0


# ─── The rejected design, kept as a regression guard ─────────────────────────


def test_scaling_input_recovers_the_bias_ordering(tmp_path):
    """Why the scaled-input sweep was removed, demonstrated on two arms.

    Cumulative over-predicts this job and the packed arm is exact. Deflating the
    stage times by the cumulative arm's own bias makes it the better arm, which is
    arithmetic and not a property of either aggregation. Any sweep built on this
    would report a "threshold" that is just the bias.
    """
    # E = 4, two stages of 1 task each, 4 s and 6 s, running concurrently for 6 s.
    runs = load_runs(_dataset(tmp_path, exec_time=6.0))
    run = runs[0]
    exact = _predict(run, _feat_with(run, "observed"))
    assert exact[_PUBLISHED] == pytest.approx(6.0)     # right
    assert exact[_CUMULATIVE] == pytest.approx(10.0)   # over-predicts by 4 s

    feat = _feat_with(run, "observed")
    deflated = {k: {"time": v["time"] * 0.6, "num_tasks": v["num_tasks"]}
                for k, v in feat.items()}
    scaled = _predict(run, deflated)
    assert abs(scaled[_CUMULATIVE] - 6.0) < abs(scaled[_PUBLISHED] - 6.0)


# ─── The confound the regime section has to show ─────────────────────────────


def test_signature_is_negative_when_the_model_under_predicts(tmp_path):
    run = load_runs(_dataset(tmp_path))[0]
    assert stage_error_signature(run) == pytest.approx(-0.5)


def test_signature_is_zero_when_the_prediction_is_exact(tmp_path):
    run = load_runs(_dataset(tmp_path, preds=(4.0, 6.0)))[0]
    assert stage_error_signature(run) == pytest.approx(0.0)


def test_shape_reports_stages_multistage_groups_and_budget_pressure(tmp_path):
    """The regime buckets differ in shape, so shape has to be measurable per run."""
    run = load_runs(_dataset(tmp_path, n_tasks=(5, 3)))[0]
    n_stages, multi, tasks_per_e = _shape(run)
    assert n_stages == 2
    assert multi == 1          # the one published group holds both stages
    assert tasks_per_e == pytest.approx(2.0)   # 8 tasks over E = 4
