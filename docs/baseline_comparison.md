# Baseline comparison

This command compares a freshly collected TPC-DS baseline against the baseline
published with the paper. Run it after `model-features` on a run that used
`--split sql`.

```
make baseline-comparison FEATURES=<results>/features/<label>/spark
```

## What it is for

A new runtime's baseline is a **new dataset**, not a reproduction. The numbers are
expected to move; that is the point of running it. What must not move is the
meaning of the columns.

The command keeps those two questions apart, and in this order:

1. **Is the extraction right?** Some columns describe the query and its input, not
   the runtime: how many stages the query has, how many bytes it read. A correct
   per-query split reproduces them whatever the runtime does. The command marks
   these `<- structural`.
2. **What did the runtime change?** Only after step 1 passes does a timing
   difference mean anything.

The per-query stage count is the sharpest check available on the split. It is an
integer, it is published for all 99 queries, and it does not depend on the
runtime. A split that puts the wrong work in a query changes it at once.

The command exits non-zero when a structural column is off by more than 1% at the
median, so it works as a gate in a campaign.

## Reading the output

```
column                            published    collected   ratio  median     p10     p90
stages in the query                 1105.00      1108.00   1.003   1.000   1.000   1.000  <- structural
input read (GB)                      892.88       889.55   0.996   0.997   0.991   0.999  <- structural
shuffle read (GB)                    865.38       852.12   0.985   1.000   0.983   1.002  <- structural
execution time (s)                  3421.88      3208.29   0.938   0.959   0.712   1.049
executor runtime (s)              123393.44    126886.63   1.028   1.032   0.952   1.117
executor CPU (s)                  106791.13    110514.25   1.035   1.038   0.953   1.099
```

- `ratio` is the ratio of the **totals** over the shared queries.
- `median`, `p10`, `p90` are over the **per-query** ratios.

Execution time is read from `harness_exec_time_s`, not `exec_time_s`. The
published column is the benchmark harness's own timer; the collected `exec_time_s`
is the SQL-execution span, which is real but smaller, because the harness also
brackets parse, analysis and optimization. Comparing those two would report a
speed-up that is really a definition change. If the collected run has no
`harness_exec_time_s` the row is skipped and the command says so — re-run
`model-features` with `--sidecar`.

Below the table it prints the execution-time agreement, using the published run
directly as the predictor of the new one (slope 1, intercept 0):

```
Execution-time agreement, published as the direct predictor (n=99)
  R^2 0.8747   r 0.9492   MAE 3.620 s   RMSE 23.106 s   bias -1.240 s
```

A high `r` with a lower `R^2` means a systematic offset rather than a loss of
correlation, so both are shown. Here they are close, and the gap between them is
one query: `query67` halved between the two runtimes. Excluding it the same
comparison gives R² 0.997 and MAE 1.34 s.

Read the two together, and prefer the median. A total is one number and one large
query can carry it: in the run above the totals say the new runtime is 6.2%
faster, while the median says 4%, and a single query (`query67`, which halved)
accounts for more than the whole difference. The p10/p90 spread is what tells you
whether a change is general or local.

The published side is filtered to the on-premises baseline
(`tsg_max_bw_gbs = 999.0`) in the same storage configuration as the collected run,
read from the collected run's own `file_format_*` and `compression_codec_*` flags.
If those are null because the constants were not supplied, the filter is not
applied and the comparison pools every storage configuration, which is wrong. Set
the constants.

## What it does not do

It compares job-level columns only. It does not match a stage to a baseline
stage, and it does not calibrate anything: that is the model repository's work.

Stage-level comparison needs a stage-to-stage alignment, and stage **ordinal** is
not one: between two runtimes an adaptive re-plan reorders and inserts stages, so
pairing by position silently compares unrelated stages. Aligning the two stage
sequences on task count and bytes read gives R² 0.972 and MAE 0.30 s over 966
pairs (0.996 and 0.23 s excluding `query67`), stable across the pairing tolerance,
but that aligner is not part of this command yet.
It is a check that the collected features are the same **kind** of thing as the
published ones, not a validation of the model.

The published dataset is read only. This command never writes into the model
repository, so the released artifact stays exactly as published.
