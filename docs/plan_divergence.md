# Plan divergence

This command measures the gap between the plan a query was submitted with and the
plan that actually ran.

```
make plan-divergence RESULTS=<campaign results dir>
make plan-divergence LOG=<run>/raw/spark/app.log.zip SIDECAR=<run>/sidecar.json LABEL=onprem
```

It reads event logs only. It writes nothing unless you set `DRIFT`.

## Why this is a measurement and not a detail

A predictor that runs before the query runs can use one plan: the plan in
`SparkListenerSQLExecutionStart`. Adaptive Query Execution then rewrites that plan
each time a shuffle materialises, because a materialised shuffle gives the
optimiser a real row count where it had an estimate. Each rewrite is re-reported in
`SparkListenerSQLAdaptiveExecutionUpdate`.

So the submitted plan is not the plan that runs. The size of that gap is a ceiling
on any submission-time prediction, and the ceiling belongs to the workload, not to
the predictor. This command puts a number on it.

## The quantity is the shuffle count

The command counts `Exchange` nodes. An `Exchange` is a shuffle, and a shuffle is
where the stage graph is cut, so the count is the stage decomposition.

Three node names look like exchanges. None of them is counted as one:

| Node | Why it is excluded |
|---|---|
| `BroadcastExchange` | A broadcast runs as its own job, not as a stage of this one |
| `ReusedExchange` | It points at an exchange that is already computed. It adds no stage |
| `ShuffleQueryStage` | It wraps an `Exchange`. The child is counted, the wrapper is not |

**Do not compare total node counts.** The executed plan carries
`ShuffleQueryStage`, `BroadcastQueryStage`, `InputAdapter`, `ColumnarToRow` and
`WholeStageCodegen` nodes that a submitted plan cannot have. Those nodes are what
materialisation and code generation add. A total node count therefore differs for
almost every query and tells you nothing. The test
`test_materialisation_wrappers_do_not_change_the_shuffle_count` holds this
property.

## What the output says

**Section 1, what the logs carry.** Per cell: how many SQL executions there are,
how many carry a submitted plan, how many rewrites happened, how many plan text
sections each submitted plan has, and whether the text carries any cardinality
annotation. This section is the gate. A log without a submitted plan supports no
feature extraction at all.

The `cardinality` column is usually zero. `physicalPlanDescription` carries
`sizeInBytes`, `rowCount` and `Statistics(...)` only when the plan was rendered in
cost mode, which a normal run does not do. Processed volume then has to come from
the plan node metadata or from a catalog query. The command says so and does not
choose for you.

**Section 2, submitted against executed.** Per cell: how many named queries drift,
in which direction, and how many shuffles are lost in total. The direction matters.
A query that executes fewer shuffles than it submitted means a predictor which
trusts the submitted plan over-counts the work.

**Section 3, does the cell decide.** Only with `RESULTS`, and only for queries
present in every cell. It first checks that the submitted plan itself is the same
in every cell, because a cell-dependent submitted plan would break the premise of
the whole comparison. It then splits the drifting queries in two:

- queries that drift in every cell, where the network cannot be the cause;
- queries whose executed count depends on the cell.

**The second group is not reported as a transport effect, and you should not read
it as one.** AQE decides on statistics that arrive when a shuffle materialises, so
its decisions are timing-dependent. With one run for each cell, run-to-run
variation and the regime are confounded. To separate them, repeat the affected
queries at the cells on either side of each change.

**Section 4, the broadcast delta.** The number of `BroadcastExchange` nodes grows
between the two plans for most queries. This is mostly **not** AQE converting a
sort merge join to a broadcast join. A dynamic partition pruning subquery is a
`SubqueryAdaptiveBroadcast` in the submitted plan and becomes a `SubqueryBroadcast`
plus a real `BroadcastExchange` once it runs. That is materialisation of something
the submitted plan already held. A real join strategy change removes shuffles, so
it appears in section 2.

## Query names

Sections 2 to 4 need names, because a query has to join across cells. Names come
from the side-car, as everywhere else in this repository: the event log does not
hold them. `make plan-divergence RESULTS=...` reads `<cell>/sidecar.json` for each
cell by itself.

The command maps a SQL execution to its stages through `SparkListenerJobStart`,
whose properties carry `spark.sql.execution.id`. The side-car then names the
execution from those stage ids. An execution that runs no job stays unnamed, which
is correct: the setup execution of a TPC-DS run is a `SetCatalogAndNamespace` and
is not a query.

## Cell order and cell constants

Cells are printed by bandwidth descending, then latency ascending. Both values come
from each cell's `constants.json` (`tsg_max_bw_gbs` and `network_latency_ms`), so
the order is derived from the campaign and not from the directory names. It is a
display order. It does not claim that bandwidth matters more than latency.

A cell with no `constants.json` reports null for both and sorts last. Nothing is
guessed.
