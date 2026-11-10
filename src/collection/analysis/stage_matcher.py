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

"""Pair each stage of one run with the same stage of another run.

The matched baseline stage is not metadata: its duration and demand rate are the
model's inputs, so a wrong pairing feeds the calibration a real value against the
wrong transport terms, and non-negative least squares cannot absorb it. Every row
still looks well formed. This module therefore has one job - decide the pairing -
and one obligation: say how much it trusts each pair.

Two properties, both measured on the published campaign (see "Why it is built this
way"), separate this from a per-stage best-match search:

**The assignment is global, one-to-one and order-preserving.** Choosing each
stage's best partner independently lets one baseline stage become the reference for
several stages, and lets stage *k* match later than stage *k+1*. Audited over the
published dataset's 64,733 hybrid rows, its own ``match_id`` reuses 1,587 baseline
stages (1,374 inside the tier the model is fitted on) and runs backwards or level
across 4,276 adjacent pairs. A Needleman-Wunsch alignment over the whole query
removes both by construction - the 60,398 ``aligned`` rows have zero of each -
because stages run in a partial order that a re-plan perturbs but does not reverse
wholesale.

**Stage position is a tie-break, never a term.** Position is genuinely informative
- within one campaign the same query keeps the same stage ordinal 89.7% of the
time - and it is the only signal that separates structurally identical stages,
which is most of the stages that read no input. But it is also the signal that
fails hardest across runtimes: between 7.3.1 and 7.3.2 only 2 of 99 queries kept a
stable stage-id set, and pairing by position put 99.6% of the squared error into
transposed pairs. So position is added at a weight that can only break a tie
between candidates whose shape already agrees, and ``ordinal_weight=0`` turns it
off for a cross-runtime comparison.

Nothing is dropped. A stage the alignment could not place still gets its best
available partner, marked ``fallback``, because a matcher that quietly discards the
awkward stages cannot surface the thing they were kept for - a shuffle regression
from slower disks was found exactly that way. Filter on ``kind`` and ``quality``;
do not filter by omission.

Shape is read from whichever of ``num_tasks``, ``input_read_gb``,
``shuffle_read_gb`` and ``shuffle_write_gb`` both sides carry, so it degrades
gracefully against a dataset that lacks some of them (the released raw stage
dataset has no per-stage shuffle).
"""

from __future__ import annotations

import argparse
import collections
import json
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

#: Shape terms and their weights. A term is skipped when the field is missing on
#: either side, and the score is renormalised over the terms that did apply, so a
#: dataset without shuffle columns is scored on what it has rather than penalised.
_SHAPE_TERMS: tuple[tuple[str, float], ...] = (
    ("num_tasks", 2.0),
    ("input_read_gb", 2.0),
    ("shuffle_read_gb", 2.0),
    ("shuffle_write_gb", 2.0),
)

#: Bonus for an exactly equal task count, on top of the ratio term. An adaptive
#: re-plan moves a task count by a percent or two; an exact match is still evidence.
_EXACT_TASKS_BONUS = 1.0

#: Shape score, out of 10, below which a pair is not worth aligning: the alignment
#: prefers to leave both stages unplaced and let the fallback pass label them.
DEFAULT_MIN_SHAPE = 6.0

#: Default tie-break weight. Small enough that it can only order candidates whose
#: shape scores are within it, which is what "tie-break" has to mean.
DEFAULT_ORDINAL_WEIGHT = 0.05

_MAX_SHAPE = sum(w for _, w in _SHAPE_TERMS) + _EXACT_TASKS_BONUS   # 9.0
_SCALE = 10.0 / _MAX_SHAPE


@dataclass(frozen=True)
class Match:
    """One stage of the right-hand run and the left-hand stage it was paired with.

    ``quality`` is the shape score out of 10, with no tie-break contribution: it
    says how alike the two stages are, not how the alignment chose between equals.
    ``kind`` is ``aligned`` (placed by the global alignment), ``fallback`` (nothing
    was left to align it with, so this is its closest available partner - reuse is
    possible here and only here), or ``unmatched`` (the other run has no stages).
    """
    right_index: int
    left_index: Optional[int]
    quality: float
    kind: str

    @property
    def is_good(self) -> bool:
        """Trustworthy enough to calibrate on."""
        return self.kind == "aligned" and self.quality >= DEFAULT_MIN_SHAPE


def _ratio(a: Any, b: Any) -> Optional[float]:
    """``min/max`` in [0, 1], or ``None`` when the term carries no information.

    A term is uninformative in two cases, and both are skipped rather than scored:
    the field is missing on either side, or **both sides are zero**. Two zeros look
    like perfect agreement but are not evidence of anything - most stages read no
    external input, so crediting that agreement lets any two of them look alike. It
    is a measured failure, not a hypothetical: it scored a pair of ``query67``
    stages 7.3 out of 10 on 11-versus-7 tasks purely because neither read input, and
    that single pair carried 99.8% of the squared error in a cross-runtime
    comparison. One side zero and the other not stays a real disagreement and scores
    0.0.
    """
    if a is None or b is None:
        return None
    a, b = float(a), float(b)
    hi = max(abs(a), abs(b))
    if hi == 0.0:
        return None
    return min(abs(a), abs(b)) / hi


def shape_score(left: dict, right: dict) -> float:
    """How alike two stages are, out of 10, using no timing and no position.

    Duration is deliberately excluded: it is the quantity the comparison exists to
    measure, so letting it choose the pairing would make any two runs look alike.
    """
    total = weight_used = 0.0
    for field, weight in _SHAPE_TERMS:
        r = _ratio(left.get(field), right.get(field))
        if r is None:
            continue
        total += weight * r
        weight_used += weight
    if weight_used == 0.0:
        return 0.0
    exact = (left.get("num_tasks") is not None
             and left.get("num_tasks") == right.get("num_tasks"))
    # Renormalise over the terms that applied, then add the exact-count bonus, so a
    # dataset missing shuffle columns is scored on the same 0-10 scale.
    scaled = total / weight_used * sum(w for _, w in _SHAPE_TERMS)
    return round((scaled + (_EXACT_TASKS_BONUS if exact else 0.0)) * _SCALE, 6)


def _ordinal_closeness(i: int, j: int, n: int, m: int) -> float:
    """1.0 when two stages sit at the same relative position, falling to 0.0."""
    if n <= 1 or m <= 1:
        return 1.0 if i == j else 0.0
    return max(0.0, 1.0 - abs(i / (n - 1) - j / (m - 1)))


def match_stages(
    left: Sequence[dict],
    right: Sequence[dict],
    *,
    ordinal_weight: float = DEFAULT_ORDINAL_WEIGHT,
    min_shape: float = DEFAULT_MIN_SHAPE,
) -> list[Match]:
    """Pair every stage in ``right`` with a stage in ``left``, in run order.

    Both sequences must already be in submission order. Returns one :class:`Match`
    per element of ``right``, in the same order.

    ``ordinal_weight`` is the tie-break strength. Leave it at the default when both
    runs come from the same runtime and plan; set it to 0.0 across runtimes, where
    stage position stops being evidence and becomes a source of transposed pairs.
    """
    n, m = len(left), len(right)
    if m == 0:
        return []
    if n == 0:
        return [Match(j, None, 0.0, "unmatched") for j in range(m)]

    shape = [[shape_score(left[i], right[j]) for j in range(m)] for i in range(n)]
    # The aligned score is the shape's excess over the floor, so a pair below it is
    # worse than leaving both stages for the fallback pass. The tie-break rides on
    # top at a weight that cannot outrank a real shape difference.
    gain = [[(shape[i][j] - min_shape) + ordinal_weight * _ordinal_closeness(i, j, n, m)
             for j in range(m)] for i in range(n)]

    # Needleman-Wunsch: maximise the total gain of an order-preserving one-to-one
    # pairing, with free gaps on both sides.
    dp = [[0.0] * (m + 1) for _ in range(n + 1)]
    for i in range(n - 1, -1, -1):
        for j in range(m - 1, -1, -1):
            dp[i][j] = max(dp[i + 1][j + 1] + gain[i][j], dp[i + 1][j], dp[i][j + 1])

    aligned: dict[int, int] = {}
    i = j = 0
    while i < n and j < m:
        if dp[i][j] == dp[i + 1][j + 1] + gain[i][j]:
            aligned[j] = i
            i += 1
            j += 1
        elif dp[i + 1][j] >= dp[i][j + 1]:
            i += 1
        else:
            j += 1

    out: list[Match] = []
    for j in range(m):
        if j in aligned:
            i = aligned[j]
            out.append(Match(j, i, shape[i][j], "aligned"))
        else:
            # Nothing was left to align this stage with. Give it its closest partner
            # anyway - reuse allowed - and say so, rather than dropping it. The same
            # tie-break applies: among candidates of equal shape, and stages that
            # read no input are usually all of equal shape, position decides.
            best = max(range(n),
                       key=lambda i2: (shape[i2][j]
                                       + ordinal_weight * _ordinal_closeness(i2, j, n, m)))
            out.append(Match(j, best, shape[best][j], "fallback"))
    return out


def match_runs(
    left: Sequence[dict],
    right: Sequence[dict],
    *,
    group_by: Callable[[dict], Any] = lambda r: r["query_id"],
    order_by: Callable[[dict], Any] = lambda r: r.get("unique_stage_id", r["stage_id"]),
    **kwargs: Any,
) -> list[tuple[dict, Optional[dict], Match]]:
    """Match two whole runs, one query at a time.

    Grouping is the caller's: pair like with like (same query, and the same storage
    configuration when a run spans several), because a stage can only correspond to
    a stage of the same query. Returns ``(right_row, left_row_or_None, match)``.
    """
    lg: dict[Any, list[dict]] = collections.defaultdict(list)
    rg: dict[Any, list[dict]] = collections.defaultdict(list)
    for r in left:
        lg[group_by(r)].append(r)
    for r in right:
        rg[group_by(r)].append(r)

    out: list[tuple[dict, Optional[dict], Match]] = []
    for key, rows in rg.items():
        rows = sorted(rows, key=order_by)
        peers = sorted(lg.get(key, []), key=order_by)
        for match in match_stages(peers, rows, **kwargs):
            partner = peers[match.left_index] if match.left_index is not None else None
            out.append((rows[match.right_index], partner, match))
    return out


def report(matches: Sequence[tuple[dict, Optional[dict], Match]]) -> list[str]:
    """A tiered summary: how much was aligned, how well, and what it is worth.

    Split by whether the *paired* stage reads external input, because that is the
    tier the transport model is calibrated on, and the tier where a bad pair costs
    something. The other tier is carried for diagnosis.
    """
    tiers: dict[str, list[tuple[dict, Optional[dict], Match]]] = collections.defaultdict(list)
    for row, partner, match in matches:
        ref = partner if partner is not None else row
        tiers["transfer" if (ref.get("input_read_gb") or 0) > 0 else "no-transfer"].append(
            (row, partner, match))

    lines = [f"{'tier':14}{'stages':>8}{'aligned':>10}{'good':>9}"
             f"{'fallback':>10}{'median quality':>16}{'% of stage time':>17}"]
    total_time = sum((r.get("stage_duration_s") or 0.0) for r, _, _ in matches) or 1.0
    for tier in ("transfer", "no-transfer"):
        group = tiers.get(tier)
        if not group:
            continue
        n = len(group)
        aligned = sum(1 for _, _, m in group if m.kind == "aligned")
        good = sum(1 for _, _, m in group if m.is_good)
        fallback = sum(1 for _, _, m in group if m.kind == "fallback")
        quality = statistics.median([m.quality for _, _, m in group])
        share = sum((r.get("stage_duration_s") or 0.0) for r, _, _ in group) / total_time
        lines.append(f"{tier:14}{n:8d}{aligned / n * 100:9.1f}%{good / n * 100:8.1f}%"
                     f"{fallback / n * 100:9.1f}%{quality:16.2f}{share * 100:16.1f}%")
    return lines


def _load(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        sys.exit(f"Error: {path} does not hold a JSON array of stage rows")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pair the stages of one run with the stages of another")
    parser.add_argument("--left", type=Path, required=True,
                        help="stage_features.json of the reference run (the baseline)")
    parser.add_argument("--right", type=Path, required=True,
                        help="stage_features.json of the run being matched to it")
    parser.add_argument("--output", type=Path, default=None,
                        help="Write the pairing as JSON here (default: report only)")
    parser.add_argument("--ordinal-weight", type=float, default=DEFAULT_ORDINAL_WEIGHT,
                        help="Tie-break strength of stage position. Use 0 across "
                             "runtimes, where position stops being evidence.")
    parser.add_argument("--min-shape", type=float, default=DEFAULT_MIN_SHAPE,
                        help="Shape score out of 10 below which a pair is left to "
                             "the fallback pass")
    args = parser.parse_args()

    for path in (args.left, args.right):
        if not path.is_file():
            sys.exit(f"Error: stage features not found: {path}")

    matches = match_runs(_load(args.left), _load(args.right),
                         ordinal_weight=args.ordinal_weight, min_shape=args.min_shape)
    print("\n".join(report(matches)))

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump([{
                "query_id": row["query_id"],
                "stage_id": row["stage_id"],
                "matched_stage_id": partner["stage_id"] if partner else None,
                "match_quality": match.quality,
                "match_kind": match.kind,
            } for row, partner, match in matches], f, indent=2)
        print(f"\nWrote {len(matches)} pairings to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
