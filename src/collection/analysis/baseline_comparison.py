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

"""Compare a freshly collected TPC-DS baseline against the published one.

A new runtime's baseline is a *new dataset*, not a reproduction: the numbers are
expected to move. What must not move is the meaning of the columns. This command
separates the two questions:

  1. **Is the extraction right?** The structural columns - how many stages a query
     has, how many bytes it read - are properties of the query and the data, not of
     the runtime. If the new pipeline splits an application into queries correctly,
     these agree with the published dataset almost exactly. If it splits wrongly,
     they do not, and no amount of plausible-looking timing will hide it.
  2. **What did the runtime change?** Only after (1) passes does a timing difference
     mean anything.

That ordering is the point. The per-query stage count is the sharpest available
check on the split, because it is an integer, it is published for all 99 queries,
and it is invariant to the runtime.

The published dataset lives in the model repository and is *read only* here.

Usage:
    python -m src.collection.analysis.baseline_comparison \
        --features <results>/features/<label>/spark \
        --published ../transport-aware-spark-model/data
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Callable, Optional

# The default is a clone of transport-aware-spark-model next to this repository.
# parents[3] is the repository root: analysis -> collection -> src -> root.
_DEFAULT_PUBLISHED = Path(__file__).resolve().parents[3].parent / "transport-aware-spark-model" / "data"
_BASELINE_MARKER = 999.0

#: Columns compared per query, as ``(collected field, published field, label,
#: structural)``. ``structural`` marks the ones that check the split rather than the
#: runtime: they describe the query and its input, so a correct extraction reproduces
#: them whatever the runtime does.
#:
#: Execution time is read from ``harness_exec_time_s``, not ``exec_time_s``. The
#: published ``exec_time_s`` is the benchmark harness's own timer, and the collected
#: ``exec_time_s`` is the SQL-execution span - a real quantity, but a smaller one,
#: because the harness also brackets parse, analysis and optimization. Comparing the
#: two would report a speed-up that is really a definition change.
_COMPARED: tuple[tuple[str, str, str, bool], ...] = (
    ("num_spark_stages", "num_spark_stages", "stages in the query", True),
    ("spark_total_input_read_gb", "spark_total_input_read_gb", "input read (GB)", True),
    ("spark_total_shuffle_read_gb", "spark_total_shuffle_read_gb", "shuffle read (GB)", True),
    ("harness_exec_time_s", "exec_time_s", "execution time (s)", False),
    ("spark_total_executor_runtime_s", "spark_total_executor_runtime_s", "executor runtime (s)", False),
    ("spark_total_executor_cpu_time_s", "spark_total_executor_cpu_time_s", "executor CPU (s)", False),
)


def _storage_of(row: dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    fmt = "parquet" if row.get("file_format_parquet") else ("orc" if row.get("file_format_orc") else None)
    for codec in ("uncompressed", "snappy", "gzip"):
        if row.get(f"compression_codec_{codec}"):
            return fmt, codec
    return fmt, None


def published_baseline(published_dir: Path, file_format: Optional[str],
                       codec: Optional[str]) -> list[dict[str, Any]]:
    """The published on-premises baseline rows for one storage configuration."""
    path = published_dir / "raw" / "measurement_summary.json"
    rows = json.loads(path.read_text())
    out = []
    for row in rows:
        if float(row.get("tsg_max_bw_gbs", 0)) != _BASELINE_MARKER:
            continue
        row_fmt, row_codec = _storage_of(row)
        if file_format is not None and row_fmt != file_format:
            continue
        if codec is not None and row_codec != codec:
            continue
        out.append(row)
    return out


def _pairs(new: dict[str, dict], old: dict[str, dict], new_field: str,
           old_field: str) -> list[tuple[float, float]]:
    """``(published, collected)`` for every query where both sides carry the field."""
    out = []
    for query_id in sorted(set(new) & set(old)):
        a = old[query_id].get(old_field)
        b = new[query_id].get(new_field)
        if a is not None and b is not None:
            out.append((float(a), float(b)))
    return out


def agreement(pairs: list[tuple[float, float]]) -> dict[str, float]:
    """R^2, MAE and RMSE of the collected values against the published ones.

    The published value is used as a direct predictor of the collected one - slope 1,
    intercept 0 - so R^2 answers "how much of the new run does the old one explain as
    it stands", not "how well would they correlate after a rescale". A high Pearson r
    with a low R^2 means a systematic offset, so both are reported.
    """
    n = len(pairs)
    x = [a for a, _ in pairs]
    y = [b for _, b in pairs]
    err = [b - a for a, b in pairs]
    xbar, ybar = sum(x) / n, sum(y) / n
    ss_res = sum(e * e for e in err)
    ss_tot = sum((v - ybar) ** 2 for v in y)
    sx = math.sqrt(sum((v - xbar) ** 2 for v in x))
    sy = math.sqrt(sum((v - ybar) ** 2 for v in y))
    return {
        "n": n,
        "mae": sum(abs(e) for e in err) / n,
        "rmse": math.sqrt(ss_res / n),
        "bias": sum(err) / n,
        "r2": 1 - ss_res / ss_tot if ss_tot else float("nan"),
        "r": (sum((a - xbar) * (b - ybar) for a, b in pairs) / (sx * sy)
              if sx and sy else float("nan")),
    }


def _percentile(values: list[float], q: float) -> float:
    """The q-th percentile of a sorted list, by nearest rank."""
    if not values:
        return float("nan")
    return values[min(len(values) - 1, int(q * len(values)))]


def compare(features_dir: Path, published_dir: Path) -> tuple[list[str], bool]:
    """Build the comparison report. Returns ``(lines, structural_ok)``."""
    jobs = json.loads((features_dir / "job_features.json").read_text())
    stages = json.loads((features_dir / "stage_features.json").read_text())
    new = {j["query_id"]: j for j in jobs}
    if len(new) != len(jobs):
        return ([f"FAIL {len(jobs)} job rows carry only {len(new)} distinct query_id "
                 f"values. A baseline must have one row per query."], False)

    sample = jobs[0] if jobs else {}
    file_format, codec = _storage_of(sample)
    old_rows = published_baseline(published_dir, file_format, codec)
    old = {r["query_id"]: r for r in old_rows}

    lines: list[str] = []
    add: Callable[[str], None] = lines.append
    add(f"Collected : {features_dir}")
    add(f"Published : {published_dir}  (tsg_max_bw_gbs={_BASELINE_MARKER}, "
        f"{file_format or '?'}/{codec or '?'})")
    add("")

    shared = set(new) & set(old)
    only_new = sorted(set(new) - set(old))
    only_old = sorted(set(old) - set(new))
    add("Coverage")
    add(f"  queries collected {len(new):4d}   published {len(old):4d}   shared {len(shared):4d}")
    if only_new:
        add(f"  only in the collected run : {', '.join(only_new[:12])}"
            f"{' ...' if len(only_new) > 12 else ''}")
    if only_old:
        add(f"  only in the published set : {', '.join(only_old[:12])}"
            f"{' ...' if len(only_old) > 12 else ''}")
    add(f"  stage rows collected {len(stages):5d}   unit kinds "
        f"{', '.join(sorted({s.get('unit_kind', '?') for s in stages}))}")
    unnamed = [q for q in new if "#exec" in q]
    if unnamed:
        add(f"  {len(unnamed)} unit(s) have no query name and cannot be matched")
    add("")

    if not shared:
        add("FAIL no query name is shared with the published baseline; nothing to compare.")
        return lines, False

    add(f"{'column':30} {'published':>12} {'collected':>12} {'ratio':>7} "
        f"{'median':>7} {'p10':>7} {'p90':>7}")
    structural_ok = True
    missing_field: list[str] = []
    exec_pairs: list[tuple[float, float]] = []
    for new_field, old_field, label, structural in _COMPARED:
        pairs = _pairs(new, old, new_field, old_field)
        if not pairs:
            add(f"{label:30} {'-':>12} {'-':>12} {'-':>7}   ({new_field} not collected)")
            missing_field.append(new_field)
            continue
        if new_field == "harness_exec_time_s":
            exec_pairs = pairs
        total_old = sum(a for a, _ in pairs)
        total_new = sum(b for _, b in pairs)
        ratios = sorted(b / a for a, b in pairs if a)
        if not ratios or not total_old:
            add(f"{label:30} {'-':>12} {'-':>12} {'-':>7}")
            continue
        median = statistics.median(ratios)
        mark = "  <- structural" if structural else ""
        add(f"{label:30} {total_old:12.2f} {total_new:12.2f} {total_new / total_old:7.3f} "
            f"{median:7.3f} {_percentile(ratios, 0.10):7.3f} "
            f"{_percentile(ratios, 0.90):7.3f}{mark}")
        # A structural column that is off by more than a per cent in the median means
        # the split put different work in the query, not that the runtime got faster.
        if structural and abs(median - 1.0) > 0.01:
            structural_ok = False
    add("")

    if exec_pairs:
        a = agreement(exec_pairs)
        add(f"Execution-time agreement, published as the direct predictor "
            f"(n={a['n']})")
        add(f"  R^2 {a['r2']:.4f}   r {a['r']:.4f}   MAE {a['mae']:.3f} s   "
            f"RMSE {a['rmse']:.3f} s   bias {a['bias']:+.3f} s")
        add("")
    if "harness_exec_time_s" in missing_field:
        add("NOTE execution time was not compared: the collected run has no "
            "harness_exec_time_s.")
        add("     exec_time_s is not a substitute - it is the SQL-execution span, and "
            "the published")
        add("     column is the harness's own timer, which also brackets parse, "
            "analysis and")
        add("     optimization. Re-run model-features with --sidecar.")
        add("")

    if structural_ok:
        add("PASS the structural columns agree with the published baseline, so the "
            "per-query split matches the published one. The timing differences are "
            "the runtime's.")
    else:
        add("FAIL a structural column disagrees by more than 1% at the median. The "
            "per-query split does not reproduce the published one; the timing "
            "differences below it are not interpretable yet.")
    return lines, structural_ok


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare collected TPC-DS baseline features against the published dataset")
    parser.add_argument("--features", type=Path, required=True,
                        help="model_features/spark directory of the collected baseline run")
    parser.add_argument("--published", type=Path, default=_DEFAULT_PUBLISHED,
                        help="The model repository's data/ directory (read only)")
    args = parser.parse_args()

    if not (args.features / "job_features.json").is_file():
        sys.exit(f"Error: no job_features.json under {args.features}")
    if not (args.published / "raw" / "measurement_summary.json").is_file():
        sys.exit(f"Error: no raw/measurement_summary.json under {args.published}")

    lines, ok = compare(args.features, args.published)
    print("\n".join(lines))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
