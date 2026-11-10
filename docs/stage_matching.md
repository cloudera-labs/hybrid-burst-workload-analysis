# Stage matching

This command pairs each stage of one run with the same stage of another run.

```
make stage-match LEFT=<baseline>/stage_features.json \
                 RIGHT=<run>/stage_features.json
```

## Why the pairing has to be right

The matched baseline stage is not a label. Its duration and its demand rate are
**inputs to the model**: `stage_duration_s_baseline` and `b_base` come straight
from it. A wrong pair therefore hands the calibration a real measurement against
the wrong transport terms, and because the fit is non-negative least squares the
error cannot be absorbed by a sign change — it moves a coefficient in a real
direction. Nothing fails, and every row still looks well formed.

So the command's job is to decide the pairing, and its obligation is to say how
much it trusts each one.

## What it does

**The assignment is global, one-to-one and order-preserving.** Choosing each
stage's best partner independently — the obvious approach — allows two things it
never checks: one baseline stage becoming the reference for several stages, and
stage *k* matching later than stage *k+1*. Audited over all 64,733 hybrid stage
rows of the published campaign, its own `match_id` does both: **1,587 baseline
stages are reused** (1,374 of them inside the tier the model is fitted on) and
**4,276 adjacent pairs run backwards or level**. A Needleman-Wunsch alignment
across the whole query removes both by construction: stages run in a partial
order that a re-plan perturbs but does not reverse wholesale. On the same data
the `aligned` tier — 60,398 of 64,733 rows — has **zero** reuses and **zero**
inversions; all 16 reuses and 504 inversions this command produces are in the
4,335 `fallback` rows, which is where the label says to expect them.

**Shape decides; timing never does.** Pairs are scored out of 10 on task count and
the input, shuffle-read and shuffle-write volumes. Duration is deliberately
excluded — it is the quantity the comparison exists to measure, so letting it
choose the pairing would make any two runs look alike.

**A term with no information is skipped, not scored.** Two missing fields, or two
zeros. Two zeros look like perfect agreement and are not: most stages read no
external input, so crediting that agreement lets any two of them look alike. This
is a measured failure, not a hypothetical — it scored two `query67` stages 7.3/10
on 11-versus-7 tasks purely because neither read input, and that single pair
carried 99.8% of the squared error in a cross-runtime comparison. One side zero
against a real value stays a real disagreement.

**Stage position is a tie-break, never a term.** Position is genuinely
informative: within a campaign the same query keeps the same stage ordinal 89.7%
of the time, and it is the only thing that separates structurally identical
stages — which is most stages that read no input. It is also the signal that fails
hardest across runtimes: between 7.3.1 and 7.3.2 only 2 of 99 queries kept a
stable stage-id set, and pairing by position put 99.6% of the squared error into
transposed pairs. So it is added at a weight that can only order candidates whose
shape already agrees.

**Set `ORDINAL_WEIGHT=0` when comparing two different runtimes.**

## Nothing is dropped

Every stage gets a partner and a label:

| `kind` | meaning |
|---|---|
| `aligned` | placed by the global alignment. One-to-one, order-preserving. |
| `fallback` | the alignment had nothing left to place it against, so this is its closest available partner. Reuse is possible here and only here. |
| `unmatched` | the other run has no stages for this query at all. |

`quality` is the shape score out of 10, with no tie-break contribution: it says how
alike the two stages are, not how the alignment chose between equals. `is_good`
means `aligned` and quality at least 6.

**Filter on `kind` and `quality`. Never filter by omission.** A matcher that
quietly discards the awkward stages cannot surface the thing they were kept for —
a shuffle regression caused by slower disks was found exactly by looking at
matched stages that carried no network transfer.

## Reading the report

```
tier            stages   aligned     good  fallback  median quality  % of stage time
transfer           612     86.6%    86.6%     13.4%            9.89            40.0%
no-transfer        496     97.4%    97.4%      2.6%           10.00            60.0%
```

The split is by whether the *paired* stage reads external input, because that is
the tier the transport model is calibrated on and the tier where a bad pair costs
something. The other tier is carried for diagnosis.

A low `aligned` share in the transfer tier is the signal to look at: it means the
plans genuinely differ, and the stages that fell through to `fallback` have no
counterpart rather than a poor one.

## What it is worth

Against the published campaign's own `match_id`, refereed by **executor CPU
time** — an attribute neither matcher reads, and one that is roughly invariant to
network conditions, so the same logical stage should agree on it:

| matcher | tier | coverage | referee |
|---|---|--:|--:|
| published `match_id` | transfer | 100% | 78.8% |
| **this command, incl. fallback** | transfer | 100% | **80.7%** |
| published `match_id` | no-transfer | 100% | 88.8% |
| **this command, incl. fallback** | no-transfer | 100% | **89.0%** |
| this command, `aligned` only | transfer | 82.1% | 80.8% |
| this command, `aligned` only | no-transfer | 91.0% | 89.2% |

Better in both tiers at equal coverage, and the `aligned` tier additionally
carries the structural guarantees.

Cross-runtime, on 7.3.1 against 7.3.2 with `ORDINAL_WEIGHT=0`, the `aligned` tier
gives stage-duration agreement of **R² 0.972, MAE 0.285 s** over 1,013 pairs.
Including the fallback pairs it is R² 0.477 — that is not a defect. It is the
honest number when nothing is excluded, and `query67`, whose plan changed between
the runtimes, is most of the difference. The label is what makes the exclusion an
explicit choice rather than a silent one.

The referee is a proxy — CPU time agreeing within 25% — not ground truth.

## Does it change the published model?

The matcher is a model input, so the test that matters is whether swapping it in
moves the published result. Re-running the whole `transport-aware-spark-model`
pipeline on the 7.3.1 data — same measurements, same formulas, same filters, only
the pairing swapped — over all four bandwidths and all three RTTs:

**The coefficients do not move.** Full-pool stratified bootstrap, 1,000
iterations:

| | β_bw | β_dynamic | β_storage |
|---|--:|--:|--:|
| paper / published `match_id` | 1.0029 ± 0.0034 | 2.0813 ± 0.0843 | 13.4954 ± 0.1824 |
| this command | 1.0024 ± 0.0034 | 2.0941 ± 0.0844 | 13.4345 ± 0.1847 |

All three stay inside the paper's 95% interval, as do the 80/20 workload-level
cross-validation means and the per-cell coefficients at each of the eleven
bandwidth × RTT cells. The VIF diagnostic is unchanged at 1.16 / 2.53 / 2.78.

**Stage-level accuracy is equal or better, at every cell.** Transfer tier, full
model, published coefficients, no rows excluded:

| | R² | MAE | n |
|---|--:|--:|--:|
| published `match_id` | 0.9788 | 0.590 s | 33,763 |
| this command | **0.9803** | **0.577 s** | 33,847 |

Restricted to the rows both pairings call good, they are identical to four
decimals (R² 0.9804, MAE 0.63 s) — the two agree wherever they agree, and the
gain is entirely in the rows where they differ.

**The no-transfer tier is where the difference is visible.** Over all rows the
published pairing scores R² 0.9618 against this command's 0.9975, and five of
eleven cells fall to ≈0.92. The paper does not report those numbers because its
`match_category` filter quarantines the 86 rows responsible — correctly. This
command does not need the quarantine: it pairs, for example,
`query67_3_25gbps_10.492ms_snappy_orc` (225 tasks) with the 225-task baseline
stage at 10.00/10, where the published `match_id` paired it with a 200-task stage
at 0.75/12 and a baseline duration 3.5x too short.

**Job-level prediction is unchanged**, to four decimals at every cell (R² 0.9921,
MAE 3.82 s). Job prediction never consults the pairing; it reads the baseline
stage signature and the coefficients, and the coefficients did not move.

Two things to know before swapping it in:

- **`is_good` is stricter than the published `match_category`.** It keeps 93.3%
  of rows where `match_category == "good"` keeps 99.7%. The 6.7% it holds back
  are `fallback` rows, and in the transfer tier they are short stages — median
  0.34 s, 1.2% of the tier's stage time, mean absolute error 0.184 s against the
  tier's 0.626 s. Excluding them *raises* the reported MAE by about 8% without
  any prediction changing. Compare MAE at matched coverage, or not at all.
- The improvement is concentrated in a small number of rows. That is the point:
  the failure this prevents is rare, silent, and expensive.
