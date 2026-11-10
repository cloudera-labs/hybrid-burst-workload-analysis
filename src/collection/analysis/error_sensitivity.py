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

"""Which layer produces job-level prediction error, the stage model or the aggregation?

Job-level error comes from two layers. The transport-aware model already reports that
its end-to-end prediction works, so re-running that would restate a published result.
What it does not report is how the two layers' errors combine, or which dominates.

**A design that was tried and rejected, recorded so it is not tried again.** An earlier
version of this module scaled the observed stage times by a factor and re-scored the
arms, reporting the scale at which the ranking changed as a sensitivity threshold. That
is circular. Each arm carries its own job-level bias (cumulative +17.8%, wave packed
+6.4%, DAG packed +0.5% on this dataset), and deflating the input simply lets each arm
take its turn at the top in order of that bias. The sweep recovered the bias ordering
and nothing else. Any estimator biased by k "wins" when its input is deflated by k.

What replaces it is an exact decomposition, with no synthetic data anywhere. For a run
and an aggregation A, writing T_A(x) for the job time A produces from stage times x:

    e_agg   = T_A(observed)  - T_observed      aggregation error, stage times perfect
    e_prop  = T_A(predicted) - T_A(observed)   stage error, propagated through A
    e_total = T_A(predicted) - T_observed    = e_agg + e_prop     exactly

Both terms are measured, the identity is algebraic, and the question "do the layers
cancel" becomes a covariance on real runs rather than a manufactured crossover.

Three sections:

  1. Paired comparison. Every arm on observed stage times, then on predicted ones. The
     same input reaches every arm, so nothing here depends on a bias assumption.
  2. The decomposition above, per arm, with the covariance term.
  3. End to end by stage-error regime, reported with shape statistics per bucket so the
     confound is visible: a run's error regime correlates with its query shape, and this
     section cannot separate the two. It is descriptive, not causal.

Usage:
    python -m src.collection.analysis.error_sensitivity
    python -m src.collection.analysis.error_sensitivity --tpcds-dir <model repo>/data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import numpy as np

from src.collection.analysis.tpcds_aggregation import (
    _CUMULATIVE,
    _DAG_PACKED,
    _DEFAULT_TPCDS,
    _PUBLISHED,
    _PUBLISHED_PRINTED,
    _job_time,
    load_runs,
)
from src.collection.analysis.verify_aggregation import _PACKED, _PRINTED, compute_metrics

_ARMS = [_CUMULATIVE, _PUBLISHED, _PUBLISHED_PRINTED, _DAG_PACKED]
_SHORT = {
    _CUMULATIVE: "cum",
    _PUBLISHED: "packed",
    _PUBLISHED_PRINTED: "printed",
    _DAG_PACKED: "DAG",
}


# ─── Feeding the arms one stage-time source or the other ─────────────────────


def _feat_with(run: dict[str, Any], source: str):
    """The run's stage features with ``time`` taken from ``source``.

    ``source`` is "observed" or "predicted". There is deliberately no scale parameter:
    see the rejected design in the module docstring.
    """
    out = {}
    for sid, f in run["feat"].items():
        if source == "predicted":
            if f["pred"] is None:
                return None
            value = f["pred"]
        else:
            value = f["time"]
        out[sid] = {"time": value, "num_tasks": f["num_tasks"]}
    return out


def _predict(run: dict[str, Any], feat) -> dict[str, float]:
    E, ov = run["E"], run["overhead"]
    return {
        _CUMULATIVE: sum(f["time"] for f in feat.values()) + ov,
        _PUBLISHED: _job_time(run["published"], feat, E, ov, _PACKED),
        _PUBLISHED_PRINTED: _job_time(run["published"], feat, E, ov, _PRINTED),
        _DAG_PACKED: _job_time(run["dag"], feat, E, ov, _PACKED),
    }


def both_predictions(runs: list[dict]) -> tuple[list[dict], np.ndarray, dict, dict]:
    """Every arm's job time from observed and from predicted stage times.

    Only runs that have a prediction for every stage are kept, so the two sides are
    scored on exactly the same set.
    """
    kept, observed = [], []
    on_obs = {a: [] for a in _ARMS}
    on_pred = {a: [] for a in _ARMS}
    for r in runs:
        f_pred = _feat_with(r, "predicted")
        if f_pred is None:
            continue
        p_obs = _predict(r, _feat_with(r, "observed"))
        p_pred = _predict(r, f_pred)
        kept.append(r)
        observed.append(r["observed"])
        for a in _ARMS:
            on_obs[a].append(p_obs[a])
            on_pred[a].append(p_pred[a])
    return (kept, np.asarray(observed, float),
            {a: np.asarray(v, float) for a, v in on_obs.items()},
            {a: np.asarray(v, float) for a, v in on_pred.items()})


# ─── 1. Paired comparison ────────────────────────────────────────────────────


def section_paired(observed, on_obs, on_pred) -> None:
    print("\n=== 1. Every arm, observed against predicted stage times ===")
    print("  The same stage times reach every arm, so this comparison assumes nothing.")
    print(f"  Runs: {len(observed)}")
    for metric in ("MAE", "R2"):
        print(f"\n  {metric}:")
        print(f"  {'Input':<26}" + "".join(f"{_SHORT[a]:>12}" for a in _ARMS))
        for label, preds in (("observed stage times", on_obs),
                             ("predicted stage times", on_pred)):
            row = {a: compute_metrics(observed, preds[a])[metric] for a in _ARMS}
            print(f"  {label:<26}" + "".join(f"{row[a]:>12.4f}" for a in _ARMS))

    mae_o = {a: compute_metrics(observed, on_obs[a])["MAE"] for a in _ARMS}
    mae_p = {a: compute_metrics(observed, on_pred[a])["MAE"] for a in _ARMS}
    for arm in (_PUBLISHED, _DAG_PACKED):
        print(f"\n  Advantage of {_SHORT[arm]} over cumulative summation: "
              f"{mae_o[_CUMULATIVE] / mae_o[arm]:.2f}x on observed stage times, "
              f"{mae_p[_CUMULATIVE] / mae_p[arm]:.2f}x on predicted")


# ─── 2. The decomposition ────────────────────────────────────────────────────


def section_decomposition(observed, on_obs, on_pred) -> None:
    print("\n=== 2. Error decomposition: e_total = e_agg + e_prop ===")
    print("  e_agg  = aggregation error with perfect stage times")
    print("  e_prop = stage-model error propagated through the aggregation")
    print(f"\n  {'Arm':<10}{'|e_agg|':>10}{'|e_prop|':>10}{'|e_total|':>11}"
          f"{'e_agg':>10}{'e_prop':>10}{'corr':>8}{'agg share':>11}")
    for a in _ARMS:
        e_agg = on_obs[a] - observed
        e_prop = on_pred[a] - on_obs[a]
        e_tot = on_pred[a] - observed
        assert np.allclose(e_tot, e_agg + e_prop), "the identity must hold exactly"
        corr = float(np.corrcoef(e_agg, e_prop)[0, 1])
        # Share of the total error variance attributable to the aggregation term.
        var_tot = float(np.var(e_tot))
        share = float(np.var(e_agg)) / var_tot if var_tot > 0 else float("nan")
        print(f"  {_SHORT[a]:<10}{np.abs(e_agg).mean():>10.3f}"
              f"{np.abs(e_prop).mean():>10.3f}{np.abs(e_tot).mean():>11.3f}"
              f"{e_agg.mean():>+10.3f}{e_prop.mean():>+10.3f}{corr:>8.3f}{share:>11.3f}")

    print("\n  A negative correlation means the two layers' errors partly cancel.")
    print("  'agg share' is Var(e_agg)/Var(e_total); above 1 means the aggregation term")
    print("  is larger than the total, which only happens when cancellation is strong.")


# ─── 3. By stage-error regime, with the confound shown ───────────────────────


def stage_error_signature(run: dict[str, Any]) -> float | None:
    """Signed relative error of the run's total stage time, predicted against observed."""
    obs = sum(f["time"] for f in run["feat"].values())
    if obs <= 0 or any(f["pred"] is None for f in run["feat"].values()):
        return None
    return sum(f["pred"] for f in run["feat"].values()) / obs - 1.0


def _shape(run: dict[str, Any]) -> tuple[int, int, float]:
    """Query shape: stages, multi-stage groups, and how far the budget is exceeded."""
    n_stages = len(run["feat"])
    multi = sum(1 for g in run["published"] if len(g["members"]) > 1)
    tasks = sum(f["num_tasks"] for f in run["feat"].values())
    return n_stages, multi, tasks / max(run["E"], 1)


_BUCKETS = [
    ("under by more than 20%", -np.inf, -0.20),
    ("under by 5 to 20%", -0.20, -0.05),
    ("within 5%", -0.05, 0.05),
    ("over by 5 to 20%", 0.05, 0.20),
    ("over by more than 20%", 0.20, np.inf),
]


def section_by_regime(kept, observed, on_pred) -> None:
    sigs = np.array([stage_error_signature(r) for r in kept], dtype=float)
    shapes = np.array([_shape(r) for r in kept], dtype=float)

    print("\n=== 3. End to end, by stage-error regime (DESCRIPTIVE, CONFOUNDED) ===")
    print("  A run's stage-error regime is not assigned; it correlates with query shape.")
    print("  The shape columns are printed so the confound is visible. This section")
    print("  cannot separate the two, and no causal reading of it is supported.")
    print(f"\n  Stage-level signed relative error: mean {sigs.mean() * 100:+.2f}%, "
          f"median {np.median(sigs) * 100:+.2f}%, "
          f"under-predicts on {(sigs < 0).mean() * 100:.1f}% of runs")

    print(f"\n  {'Bucket':<26}{'n':>6}" + "".join(f"{_SHORT[a]:>10}" for a in _ARMS)
          + f"{'best':>9}{'stages':>9}{'multi':>8}{'tasks/E':>9}")
    for label, lo, hi in _BUCKETS:
        sel = (sigs >= lo) & (sigs < hi)
        if sel.sum() < 30:
            continue
        obs_b = observed[sel]
        mae = {a: compute_metrics(obs_b, on_pred[a][sel])["MAE"] for a in _ARMS}
        best = _SHORT[min(_ARMS, key=lambda a: mae[a])]
        sh = shapes[sel]
        print(f"  {label:<26}{int(sel.sum()):>6}"
              + "".join(f"{mae[a]:>10.3f}" for a in _ARMS)
              + f"{best:>9}{np.median(sh[:, 0]):>9.0f}"
              f"{np.median(sh[:, 1]):>8.0f}{np.median(sh[:, 2]):>9.1f}")

    # Within one shape stratum, does the bucket ordering survive?
    med = np.median(shapes[:, 0])
    print(f"\n  Within one shape stratum only (runs with more than {med:.0f} stages),")
    print("  which removes the largest part of the confound:")
    print(f"  {'Bucket':<26}{'n':>6}" + "".join(f"{_SHORT[a]:>10}" for a in _ARMS)
          + f"{'best':>9}")
    for label, lo, hi in _BUCKETS:
        sel = (sigs >= lo) & (sigs < hi) & (shapes[:, 0] > med)
        if sel.sum() < 30:
            continue
        obs_b = observed[sel]
        mae = {a: compute_metrics(obs_b, on_pred[a][sel])["MAE"] for a in _ARMS}
        best = _SHORT[min(_ARMS, key=lambda a: mae[a])]
        print(f"  {label:<26}{int(sel.sum()):>6}"
              + "".join(f"{mae[a]:>10.3f}" for a in _ARMS) + f"{best:>9}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Which layer produces job-level prediction error?")
    parser.add_argument("--tpcds-dir", type=Path, default=_DEFAULT_TPCDS)
    args = parser.parse_args()

    runs = load_runs(args.tpcds_dir)
    if not runs:
        sys.exit("Error: no TPC-DS runs joined. Check --tpcds-dir.")
    kept, observed, on_obs, on_pred = both_predictions(runs)
    print(f"TPC-DS runs joined: {len(runs)}, with a full set of predictions: {len(kept)}")

    section_paired(observed, on_obs, on_pred)
    section_decomposition(observed, on_obs, on_pred)
    section_by_regime(kept, observed, on_pred)


if __name__ == "__main__":
    main()
