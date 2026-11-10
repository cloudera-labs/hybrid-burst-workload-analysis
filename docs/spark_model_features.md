# Spark model features

This command reads the Spark event logs and writes the stage and job features
that the transport-aware Spark model needs. It uses the plain event log only. It
does not need the custom listener that the model project used before.

The model and the paper it comes from are public:

- Paper: <https://ieeexplore.ieee.org/document/11569730>
- Model code: <https://github.com/cloudera/transport-aware-spark-model>

Run the download command first. Then run:

```
make model-features
```

`ENGINE` defaults to `spark`, so `make model-features ENGINE=spark` is the same.
The model targets Spark only, so there is no Impala variant.

It reads `data/raw/spark/*.log.zip` and writes three files to
`data/model_features/spark/`:

- `stage_features.json`: one row for each stage.
- `job_features.json`: one row for each application.
- `graph_features.json`: one row for each application — the two stage-dependency
  group graphs the model's job path uses, one built from the DAG and one from the
  observed overlap. See "Stage dependency graph" below.

There is one row for each **unit**, and `query_id` is the same in all three files,
so they join on it. What a unit is depends on `--split`; see "The unit of
analysis" below. This command does not group repeated runs, and it does not match
a stage to a baseline run. That work is for a later command.

## Stage fields

Every field below comes straight from the event log. Each value is real.

| Field | Meaning |
|---|---|
| `query_id`, `app_id` | The application id (the per-run key). |
| `app_name` | The application name. |
| `stage_id`, `unique_stage_id` | The Spark stage id. |
| `total_stages` | The number of stages in the run. |
| `stage_duration_s` | The stage time, in seconds (completion − submission). |
| `stage_task_span_s` | The same stage under the published dataset's definition: the task span. See "Two measurements that have two definitions". Null when the stage reported no tasks. |
| `resource_wait_s` | The wait for an executor slot inside the stage: first task launch − submission. It is a part of `stage_duration_s`, not an addition to it. |
| `num_tasks` | The number of tasks in the stage. |
| `avg_task_duration_s` | The mean task time, in seconds. |
| `executor_runtime_s` | The executor run time for the stage, in seconds. |
| `executor_cpu_time_s` | The executor CPU time for the stage, in seconds. |
| `input_read_gb` | The input read by the stage, in GB (1 GB = 1e9 bytes). |
| `number_of_files_read` | The files read. See the note below. |
| `static_number_of_files_read` | Always null. The event log has no per-stage value. |
| `number_of_partitions_read` | The input partitions read. See the note below. |
| `spark_num_executors` | The peak number of executors at the same time. |
| `spark_executor_cores` | The cores for each executor. |
| `spark_executor_memory_gb` | The memory for each executor, in GB. |

### The file and partition counts

Spark reports the file count and the partition count for a scan as a query-level
value (from the driver), not a per-stage value. The command attributes that value
to a stage in two steps, the same way the earlier project did it:

1. It reads the value from the driver events (`SparkListenerDriverAccumUpdates`
   and the adaptive metric updates).
2. It finds the stage. First it uses the per-task records, if the Spark version
   writes the metric for each task. If not, it matches the scan node's name to the
   stage through the stage's `RDD Info` scope name.

Then:

- `number_of_files_read` and `static_number_of_files_read`: the attributed driver
  value. If neither is present but the stage is a pure scan (it reads input and
  does no shuffle read), the command uses the task count as the file proxy. One
  count is filled from the other when only one is present.
- `number_of_partitions_read`: the attributed driver value, or null.

A stage gets a count only if it runs a partitioned-table scan. A job with no such
scan has null here, which is correct. Across the full history (6,380 jobs, 53,768
stages), about one job in nine (11%) had at least one stage with a partition
count; the rest read no partitioned tables.

## The job time fields

The wall-clock of a run splits three ways, and the parts sum exactly. This is the
same split the ranking and the summary use:

    wall_clock_s = resource_wait_s + planning_startup_s + active

- `wall_clock_s`: the whole run, from application start to application end.
- `resource_wait_s`: the wait before the first executor arrived (clamped so the
  three parts add up). Nothing could run yet.
- `active`: the stage span — the last stage completion minus the first stage
  submission.
- `planning_startup_s`: what is left — admission, planning, the driver, and the
  fetch.

The model scores its job prediction against `exec_time_s`. We set it to the
**resource-wait-free** time:

    exec_time_s = active + planning_startup_s = wall_clock_s − resource_wait_s

Resource wait is a queueing effect of the busy on-prem cluster, not something the
transport-aware model predicts, so it is kept out of `exec_time_s`. For the same
reason the group graph's `scheduler_overhead_s` is the `planning_startup_s` value
only, never the resource wait.

Note for multi-job applications: `active` is the whole span from the first stage
to the last, so any idle gap between two Spark jobs falls inside `active`, not in
`planning_startup_s`. This matches the ranking and the summary.

## Stage dependency graph

`graph_features.json` holds **two** group graphs for each run. The model's job path
(`predict_graph_groups.py`) uses a group graph to add up stage predictions in a
dependency-aware way, instead of a plain sum. The two graphs cover the same stages
and differ only in how the groups are formed. Each row has:

| Field | Meaning |
|---|---|
| `query_id`, `app_id` | The application id (the join key to the other two files). |
| `name`, `source` | The application name and id (for reference). |
| `scheduler_overhead_s` | The job `planning_startup_s` (see above). The model adds it once at the job level. |
| `stages` | The stage ids in the run. |
| `stage_edges` | The stage DAG: `[parent, child]` pairs, from each stage's `Parent IDs`. |
| `dag_groups` | The DAG level groups (see below): `gid`, `members`, `kind`, `parents`. |
| `dag_group_edges` | `stage_edges` lifted to the `dag_groups` level. |
| `dag_stage_to_group` | The `dag_groups` id for each stage. |
| `observed_groups` | The wall-clock overlap groups (see below), same record shape. |
| `observed_group_edges` | `stage_edges` lifted to the `observed_groups` level. |
| `observed_stage_to_group` | The `observed_groups` id for each stage. |
| `groups`, `group_edges`, `stage_to_group` | An alias of the `observed_*` set, kept so older consumers keep working. |

Keep both graphs. They answer different questions, and mixing them up is the
easiest way to report a result that does not hold.

**`dag_groups` is the published rule** (IEEE Access, Section IV-E). Stages that
share a dependency level in `stage_edges` form one group, where the level is the
longest path from a root. It reads no timing, so it can be built before the job
runs, and a predictor may use it.

Watch one property of the input: Spark's `Parent IDs` records shuffle dependencies
**inside one Spark job only**. Two actions in one application therefore look
independent here even when they ran strictly one after the other. In the 6,380-job
history collected so far, 89% of runs have no `stage_edges` at all (median 2 stages
and 0 edges per run), so the level rule puts nearly every stage on level 0 and
treats the run as fully concurrent. A single TPC-DS query is one action, so this
case does not appear in the paper's benchmark.

**`observed_groups` comes from wall-clock time overlap.** Two stages join the same
group when their run windows (`[submission, completion]`) overlap. A group is
always kept an antichain: two stages where one depends on the other (directly or
through the DAG) are never put in the same group, so a wide wave that mixes parents
and children is split into separate parallel groups linked by
`observed_group_edges`. A stage with no timing stays on its own.

This graph reads the observed timeline, so it is an **oracle**. It measures how
much a perfect concurrency structure would be worth. Never present it as a
prediction result.

In both graphs a group of more than one stage is `"parallel"` and a lone stage is
`"sequential"`. The model treats a parallel group as overlapping work (the group
time is the longest member, or a wave-packed time when the tasks exceed the core
budget) and a sequential group as its own stage time.

## Verifying the aggregation

`make verify-aggregation` checks the group graphs. It is a check, not a predictor.
It reads the three feature files above and asks one question: given the real
per-stage times, how close do the aggregation methods come to the real job time?

Two choices are independent here: how the groups are formed, and how a group is
timed when its tasks exceed the core budget `E`. The arms cover the combinations
that matter. Every arm adds `scheduler_overhead_s`:

- **Cumulative sum**: every stage time added up. The baseline, after Mustafa et
  al., and the arm the paper compares against.
- **DAG, printed Eq.**: the `dag_groups` times added up, with the saturated case
  exactly as Eq. (`access.tex:514-517`) prints it, the sum of the member stage
  times. This is the paper as written.
- **DAG, wave packed**: the same groups, but the saturated case as the model code
  computes it. This is the paper as implemented, and the only grouped arm a
  predictor may use, because it needs no timing.
- **Observed, packed**: the `observed_groups` times, wave packed. An oracle, so
  read it as a ceiling.
- **One group (control)**: every stage placed in a single group, which assumes
  total concurrency and ignores the DAG. It separates what the dependency
  structure buys from what the core-budget wave packing buys on its own. When this
  arm matches a DAG arm, `stage_edges` carried no usable structure for that set of
  runs.

A lone group is its own stage time, and a group whose tasks fit the budget is the
longest member. Those two cases do not depend on the saturated-case variant, so the
two DAG arms differ only where a group exceeds `E`.

The grouping math is the published model's own aggregation
(`predict_graph_groups.py`), used without change. The one difference is the
input: the real `stage_duration_s` stands in for the model's predicted stage
time, because this command measures the aggregation and the graph, not a network
model. The core budget is `spark_num_executors × spark_executor_cores`.

It prints two tables. The first is the global result over all runs. The second
breaks the same metrics down by observed runtime (`0.3–10`, `10–30`, `30–100`,
`100+` seconds). The metrics are the model's: MAE, RMSE, MAPE%, WMAPE%, and R2.
The command writes nothing.

```
make verify-aggregation
```

It evaluates each run on its own; it does not group repeated runs, and it uses
every run that has both a graph and its stages. Run `make model-features` first.

## Does the concurrency structure transfer between runs?

`make group-stability` answers the question `verify-aggregation` leaves open. The
`observed_groups` arm above wins, but it reads the timeline of the very run it is
scoring, so it is a ceiling rather than a predictor. This command asks whether the
same grouping can be taken from an **earlier execution of the same application**,
which a predictor holding historical profiles already has.

It builds families of repeated runs, keyed on `(app_name, stage count)` so a changed
plan is not compared against itself, orders them by the sequence number in the YARN
application id so the donor always precedes the recipient, and scores each recipient
with the donor's group membership but its own stage times and its own `E`. Two donor
policies are reported: the most recent earlier run, and the family's first run held
fixed.

Besides the arm table it prints the recovered fraction of the gap between the DAG
arm and the own-run overlap arm, a subset breakdown so a high recovery can be
checked against the harder runs, how often the transferred partition is *identical*
to the recipient's own, and two integrity counts: pairs whose stage id sets differ,
and transferred groupings that place a stage in the same group as one of its own
descendants.

```
make group-stability
```

## Re-deriving the TPC-DS tables

`make tpcds-aggregation` reproduces every TPC-DS figure the aggregation study
reports, so none of them depends on a throwaway script: the arms on the benchmark,
the two saturated-case forms and how often they diverge, signed error for both,
stage-graph sparsity against the production trace, the arms re-scored inside each
interconnect configuration, and the transfer-share dilution control.

It reads the dataset released with the transport-aware model and never writes to
it. `TPCDS_DIR` points at that repository's `data` directory.

```
make tpcds-aggregation
make tpcds-aggregation TPCDS_DIR=/path/to/transport-aware-spark-model/data
```

Run `make model-features` first if you want the production row of the sparsity
table; without it that row is skipped and the TPC-DS row still prints.

## Which layer produces job-level error?

`make error-sensitivity` splits job-level prediction error into its two sources. For a
run and an aggregation `A`, writing `T_A(x)` for the job time `A` produces from stage
times `x`:

```
e_agg   = T_A(observed)  - T_observed      aggregation error, stage times perfect
e_prop  = T_A(predicted) - T_A(observed)   stage-model error, propagated through A
e_total = T_A(predicted) - T_observed    = e_agg + e_prop     exactly
```

Both terms are measured and the identity is algebraic, so "do the two layers' errors
cancel" becomes a covariance on real runs. It also prints a paired comparison of every
arm on observed against predicted stage times, and an end-to-end breakdown by
stage-error regime.

**Read the third section as descriptive only.** A run's stage-error regime is not
assigned, and it correlates with query shape, so the section prints shape statistics
per bucket to make the confound visible. It cannot separate error from shape.

```
make error-sensitivity
```

**A rejected design, recorded so it is not repeated.** An earlier version scaled the
observed stage times by a factor and reported the scale at which the arm ranking
changed as a sensitivity threshold. That is circular: each arm carries its own
job-level bias, so deflating the input lets each arm take its turn at the top in order
of that bias, and the sweep recovers nothing but the bias ordering.
`test_scaling_input_recovers_the_bias_ordering` guards against reintroducing it.

## The unit of analysis

The model's row is a **query**: one job dependency graph, matched to its
on-premises baseline by `query_id`. In production one query is also one Spark
application, so the two are the same thing and nothing is needed.

A benchmark harness breaks that. The TPC-DS driver runs all 99 queries inside one
long-lived application. Read with the default setting, that whole run becomes a
single 3208-second "query", and the per-query structure the model calibrates on
is gone.

`--split` chooses the unit:

| Value | One row per | Use it for |
|---|---|---|
| `none` (default) | Spark application | Production. An application is a query. |
| `sql` | SQL execution | A benchmark that runs many queries in one application. |

`sql` cuts the application on `SparkListenerSQLExecutionStart` and `End`. Every
stage carries `spark.sql.execution.id` in its submission properties, so the
partition is **read from the log**, not guessed from timing. An execution that
produced no stages (the driver's opening statement, a DDL served from the
metastore) is dropped: it has no job graph to model.

Each unit gets its own job row, its own stage rows, and its own group graph. Two
extra columns record where a row came from: `unit_kind` (`application` or
`sql_execution`) and `unit_id` (the application id, or the SQL execution id).

### The side-car

Everything this command measures comes from the event log. Two things about a
benchmark run are not in it, and those come from a side-car:

- **The query name.** The driver issues all 99 queries from one call site, so
  every execution's description is the same string
  (`foreach at SparkSQLExecutor.java:89`).
- **The harness's own execution timer.** It brackets more than the SQL
  execution, so no event log can reconstruct it. See `harness_exec_time_s` below.

```
make tpcds-sidecar METRICS=<run>/raw/metrics/<run>-metrics.tar.gz \
                   SIDECAR=<run>/sidecar.json
```

That reads the benchmark's own metrics bundle, which records `query_name`,
`stage_id` and `execution_timer` on its task records, and writes
`{"by_stage": {...}, "exec_time_s": {...}}`. Pass it with `--sidecar`.

Without it, a unit is keyed `<app_id>#exec<n>` and `harness_exec_time_s` is null:
traceable, joinable, and impossible to match to a baseline, which is the right
failure mode. Nothing is invented.

A stage map that puts two query names inside one SQL execution is reported and
the unit is left unnamed. That disagreement means the side-car and the log
describe different runs, and a wrong `query_id` would silently put the run in the
wrong baseline group. Disagreeing timer readings for one query are reported the
same way, not averaged.

The side-car is optional and benchmark-specific. A production run needs none:
`--split none` with no side-car reads everything from the event log.

## Two measurements that have two definitions

The published dataset measured a stage and a query differently from the obvious
event-log reading. Both readings are real and both are kept. **No existing column
changed meaning**; the published definitions are emitted alongside, under their
own names.

| Question | This command's own column | The published dataset's definition |
|---|---|---|
| How long did a stage take? | `stage_duration_s` — submission to completion | `stage_task_span_s` — the task span |
| How long did a query take? | `exec_time_s` — the unit's resource-wait-free wall clock | `harness_exec_time_s` — the harness's timer |

**`stage_task_span_s`** is the latest task finish minus the earliest task start,
where a task's start is *reconstructed* as
`finish − (executor_runtime + deserialize_time)`. So it excludes the pending wait
before the first task, and the scheduler delay and result fetch inside each task;
`stage_duration_s` includes all of it. On the 7.3.2 baseline the difference is
large — 3607 s against 6483 s over 1108 stages — so the two are not
interchangeable. It is computed from the event log, and it is null for a stage
that reported no tasks.

**`harness_exec_time_s`** comes from the side-car, because the harness starts its
timer before Spark posts anything: it includes parse, analysis and optimization,
and any job the driver ran before `SQLExecutionStart`. It is consistently the
larger of the two — 3299 s against 3208 s over 99 queries, and up to 8 s on a
single query.

**Use the published definitions when comparing to the published dataset**, and
either when comparing runs to each other, as long as you use the same one on both
sides. `make baseline-comparison` uses `harness_exec_time_s` and says so if the
column is missing.

### Verified against the published campaign

The split was checked against the dataset the paper released, on the 7.3.2
on-premises baseline (`application_1788681512191_0011`, 99 queries in one
application). The check is `make baseline-comparison`, and it separates two
questions on purpose.

The **structural** columns describe the query and its input, not the runtime, so
a correct split reproduces them whatever the runtime does. They are the sharpest
available test of the split, and the per-query stage count is the sharpest of
those: it is an integer, it is published for all 99 queries, and a wrong split
changes it immediately.

| Column | Ratio of totals | Per-query median |
|---|---|---|
| stages in the query | 1.003 | 1.000 |
| input read | 0.996 | 0.997 |
| shuffle read | 0.985 | 1.000 |

All 99 query names are recovered, and no stage lands in two queries. Only after
that passes does a timing difference mean anything.

Execution-time agreement, with the published run used directly as the predictor
of the new one: **R² 0.875, r 0.949, MAE 3.62 s** over the 99 queries. One query
carries almost all of it — `query67` halved between the runtimes — and without it
the two runs agree to **R² 0.997, MAE 1.34 s**.

## The deployment and campaign constants

The model uses more inputs than any event log holds, because they describe the
network, the operating system, the dataset and the cluster, not the job. None of
them is a hidden Spark setting. You can read each one with a command.

The first four are the transport constants. The stage model cannot run without
them:

| Field | Model role | How to get it |
|---|---|---|
| `window_max_bytes` | TCP window `W_max` | `sysctl net.ipv4.tcp_rmem` (the third value), or `net.core.rmem_max`. |
| `mtu` | Initial window `W_init` | `ip link show <iface>`, or `cat /sys/class/net/<iface>/mtu`. |
| `network_latency_ms` | Round-trip time | Measure the round-trip time to the cloud side with `ping`, or use the link spec. |
| `tsg_max_bw_gbs` | Link bandwidth `B_link` | The network link capacity, in Gb/s. |

The rest are the campaign constants. A run can be scored without them, but it
cannot be **matched** to its baseline run or pooled with the published rows,
because the storage and cluster configuration is what the matching keys on:

| Field | How to get it |
|---|---|
| `data_size_gb` | The physical size of the dataset under test. |
| `file_format` | `parquet` or `orc`. Written out as the one-hot flags the published dataset uses. |
| `compression_codec` | `uncompressed`, `snappy`, or `gzip`. Also one-hot. |
| `num_workers` | The number of worker nodes. |
| `worker_disk_bw_gbps`, `worker_net_bw_gbps` | Per-worker bandwidths. The aggregate `total_worker_*` columns are derived from these and the worker count. |
| `tsg_avg_bw_gbs`, `tsg_transfer_gb` | Observed use of the interconnect. Both are `0.0` on an on-premises baseline, and come from the gateway's counters on a shaped cell. |

`worker_avg_cpu` is in the published dataset but is **always null** here. It is a
node-level measurement, not a constant, and no Spark event log observes it; it
needs a sampler on the workers. The column is kept so the schema lines up rather
than the field vanishing without explanation.

Every field is null unless you supply it. When you set one, the command writes it
onto every row. Nothing is made up: an unset value stays null, and the command
prints which fields are unset, transport and campaign constants reported
separately.

The recommended way is one file per grid cell, committed with the run:

```json
{
  "window_max_bytes": 134217728,
  "mtu": 9001,
  "network_latency_ms": 0.179,
  "tsg_max_bw_gbs": 999.0,
  "data_size_gb": 325.86,
  "file_format": "parquet",
  "compression_codec": "uncompressed",
  "num_workers": 3,
  "worker_net_bw_gbps": 6.25,
  "tsg_avg_bw_gbs": 0.0,
  "tsg_transfer_gb": 0.0
}
```

```
python -m src.collection.spark.model_pipeline --input <run>/raw/spark \
  --output <run>/features/spark --split sql --labels <run>/raw/labels.json \
  --constants <run>/constants.json
```

A file is also the only form that survives a campaign Makefile: under GNU Make
3.81 a variable exported with the `export` directive is **not** visible to
`$(shell ...)`, so constants passed that way expand to nothing and every row is
written null. A path is an ordinary Make variable and has no such problem.

The `SPARK_*` environment variables still work, and an explicit flag still wins.
The order is: flag, then `--constants` file, then environment.

```
SPARK_WINDOW_MAX_BYTES=134217728
SPARK_MTU=9001
SPARK_NETWORK_LATENCY_MS=10
SPARK_TSG_MAX_BW_GBS=25
```
