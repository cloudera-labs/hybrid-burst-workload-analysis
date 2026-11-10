# Guidance for coding agents

This file guides any coding agent that works in this repository. `AGENTS.md` is a
link to this file.

## First: check the environment

Before you run any command in this repository, check the `.env` file.

1. If `.env` does not exist, or a value is still a placeholder, do not guess.
2. Ask the user for each missing value. Ask for the Cloudera Manager URL, the
   cluster name, the user name, the password, and the Spark History Server URL.
   The tools use basic authentication only.
3. Copy `.env.example` to `.env`. Write the values the user gives you.
4. Never print a password. Never commit `.env`.

The Makefile reads `.env` and passes the values to every command. The tools read
these values from the environment.

## What this repository does

This repository reads the Impala and Spark workload history from a cluster. It
ranks the workloads by how much a compute-and-storage split affects each one. It
also prints a summary: the data volume, the network rate, the risk mix, and the
compute you can move. For Impala it also prints the memory and the smallest node
count.

## The commands

`ENGINE` selects the workload: `impala` or `spark`. Spark is the default. Run
`make help` for the full list. The commands are:

- `make download ENGINE=impala|spark`: download the last `DAYS` days of raw data
  to `data/raw/<engine>`. `DAYS` defaults to 7.
- `make process ENGINE=impala|spark`: make a small summary in
  `data/processed/<engine>` from all the raw data.
- `make ranking ENGINE=impala|spark`: rank the processed data.
- `make summary ENGINE=impala|spark`: print the workload summary from the
  processed data.
- `make model-features`: read `data/raw/spark` into the feature JSON the
  transport-aware model consumes. Spark only. `SPLIT` selects the unit of
  analysis, and getting it wrong is silent, so read the note below.
- `make tpcds-sidecar`: extract the query names and the harness's own per-query
  timer for a TPC-DS run from the benchmark's metrics bundle. The event log
  carries neither.
- `make baseline-comparison`: check a collected TPC-DS baseline against the
  published one. Non-zero exit on a structural mismatch.
- `make stage-match`: pair the stages of two runs. The matched baseline stage is a
  model input, not a label, so read `docs/stage_matching.md` before changing it.

The `ranking` and `summary` commands read the processed data. Run `download` and
`process` first.

## The unit of analysis

The model's row is a query. In production one query is one Spark application, so
`SPLIT=none` (the default) is correct and there is nothing to think about.

A benchmark harness is different. The TPC-DS driver runs all 99 queries inside a
single application, so the default reads that whole run as one 3208-second
"query" and the structure the model calibrates on is gone. Use `SPLIT=sql`, which
cuts the application on its SQL executions, and `LABELS` to name them.

This matters because both readings produce well-formed output. Nothing fails.
`make baseline-comparison` is the check: it compares the structural columns, which
describe the query rather than the runtime, against the published dataset.

## Measure from the event log, name from the side-car

Every value this repository *measures* comes from the Spark event log, and that is
the point: the published study measured its stages with a bespoke `SparkListener`
JAR, which cannot be attached to somebody's production job. Keep it that way.

Two things about a benchmark run are genuinely not in the event log, and only
those come from the harness's metrics bundle: the query **name** of each unit, and
the harness's own **execution timer**. Both are optional and benchmark-specific; a
production run uses neither.

When the published dataset measured something differently, **add a column, never
redefine one** - `exec_time_s` and `harness_exec_time_s` are both real and both
kept. A silently redefined column is indistinguishable from a performance change.

**The exception, and why it is one.** `stage_duration_s` in the STAGE features
is the task span, matching the published column of that name
(`parse_queries.py:200-207`, each task's start reconstructed as
`finish - (executor_runtime + deserialize)`). It briefly carried
submission-to-completion alongside a separate `stage_task_span_s`; both are now
gone, and only the published meaning is emitted.

Two columns were kept for a while and it was the wrong call here, because they
were not two readings of one thing. Submission-to-completion also counts a
stage pending for a task slot held by its own siblings, and that quantity is
not additive: on the SF=1000 baseline, query4's stages carry 724 s of it inside
a query that ran for 169 s, because its stages overlap about 5.6x. It also is
not what the stage-level `resource_wait_s` claimed - the JOB-level resource
wait is 0.000 s on all 99 queries, so nothing ever waited for an executor. A
column that cannot be summed and does not mean its name is not a second
reading; it is a trap, and it cost a calibration run.

The JOB-level `resource_wait_s` stays. That one is real.

See `docs/spark_model_features.md`.

## The layout

- `src/collection/`: the Python modules, grouped by concern into subpackages:
  `common/` (shared CM client), `impala/` (fetcher, parser, pipeline), `spark/`
  (downloader, event_parser, pipeline, model_parser, model_pipeline,
  run_splitter, tpcds_sidecar), and `analysis/` (ranking, summary,
  baseline_comparison, stage_matcher).
- `tests/`: the tests. Run them with `make test`.
- `docs/`: how to read the output of each command.

## Rules

- Use GET only against the Cloudera Manager API. Never POST, PUT, PATCH, or
  DELETE.
- Never invent a value the source does not carry. A constant nobody supplied stays
  null; a query nobody named keeps a traceable key. An output row that looks
  complete but holds a guessed independent variable is worse than one that is
  visibly incomplete, because nothing downstream can tell.
- Keep benchmark knowledge out of the parser. The event log tells you the
  structure; a side-car tells you the names. A second benchmark should need its
  own extractor next to `tpcds_sidecar.py`, and no change to `model_parser.py`.
- Never redefine an existing column. If a comparison needs a different definition
  of the same quantity, emit it under its own name and document both.
- Keep every credential in `.env`. Never write a credential into the code or a
  log.
- Write the documents in Simplified Technical English. Use short sentences and
  plain words.
