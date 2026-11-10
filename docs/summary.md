# Summary

## What the command does

The summary command reads the processed data and prints one overview of the
workload. It shows the data volume, the network rate, the risk mix, and the
compute you can move to the cloud. Both engines print the time breakdown and the
bursting potential. For Impala it also prints the memory and the node count. For
Spark it prints the executor use.

```bash
make summary ENGINE=impala   # reads data/processed/impala
make summary ENGINE=spark    # reads data/processed/spark
```

Run the download and process commands first. See [the data pipeline](pipeline.md).

The command reads only the processed data. It does not call the cluster.

## The printed output

```
── Workload summary (impala) ──
  Data range:      <from> → <to>  (<span>)
  Executions:      <n>

── Data volume ──
  Total read:      <n> TB
  Total written:   <n> TB
  Average rate:    <n> Gbit/s  (read + written over the data range)
  Peak rate:       <n> Gbit/s  (busiest single workload)

── Disaggregation risk ──
  Low:      <n> (<p>%)   read=<n>  written=<n>
  Medium:   <n> (<p>%)   read=<n>  written=<n>
  High:     <n> (<p>%)   read=<n>  written=<n>

── Compute you can offload ──
  Total CPU-time:  <n> CPU-seconds  (<n> CPU-hours)
  Low-risk share:  <p>%  (move this share of the compute to the cloud)

── Time breakdown ──
  Wall-clock:        <n> h  (summed over all executions)
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  Resource wait:     <n> h  (<p>%)  queued for admission, nothing runs
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  Planning/startup:  <n> h  (<p>%)  compile/plan, then client result fetch
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  Active processing: <n> h  (<p>%)  the query actually executing
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  With enough admission capacity the resource wait approaches zero, leaving <p>% of wall-clock.

── Bursting potential ──
  Reclaimable:        <n> h  (<p>% of wall-clock, ideal case)
    Move Low:         <n> h  (<p>%)  Low wall-clock, offloaded to the cloud
    Move Medium:      <n> h  (<p>%)  Medium wall-clock, offloaded (test first)
    Relieve High:     <n> h  (<p>%)  High admission wait the freed capacity may lower

── Memory and node count ──          (Impala only)
  Peak concurrent memory:  <n> GB
  Memory per node:         <n> GB  (the largest query seen, plus 10%)
  Smallest node count:     <n> nodes at 0% headroom
  Smallest node count:     <n> nodes at 30% headroom
```

The time breakdown and the bursting potential use the same three phases for Spark,
with Spark wording. Spark also prints an executor-use section instead of the memory
and node count:

```
── Time breakdown ──                 (Spark wording)
  Wall-clock:        <n> h  (summed over all executions)
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  Resource wait:     <n> h  (<p>%)  no executor yet, nothing runs
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  Planning/startup:  <n> h  (<p>%)  admission, planning, driver, fetch
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  Active processing: <n> h  (<p>%)  stages actually running
    Low risk:        <n> h  (<p>%)
    Medium risk:     <n> h  (<p>%)
    High risk:       <n> h  (<p>%)
  With enough executors the resource wait approaches zero, leaving <p>% of wall-clock.
  Executor-slot contention inside active: <n> h (<p>% of wall-clock) would also shrink.

── Bursting potential ──             (Spark wording)
  Reclaimable:        <n> h  (<p>% of wall-clock, ideal case)
    Move Low:         <n> h  (<p>%)  Low wall-clock, offloaded to the cloud
    Move Medium:      <n> h  (<p>%)  Medium wall-clock, offloaded (test first)
    Relieve High:     <n> h  (<p>%)  High wait the freed slots may lower (<n> h resource wait + <n> h contention)

── Executor use ──                   (Spark only)
  Peak executors:  <n>  (the most one application used at the same time)
  Executor-time:   <n> executor-hours
  Core-time:       <n> core-hours
```

## How to read it

### Data volume and rate

- **Total read / written**: the bytes the workload moved in the window. This is
  the load that goes across the network after you separate compute from storage.
- **Average rate**: the total bytes divided by the time span. It is the mean
  network rate across the whole window.
- **Peak rate**: the highest rate for one workload. Use it to size the network
  link for the busiest moment, not just the average.

Compare the rate with your network link. A rate below your link means you have
room to move more work. A rate above your link means you move only part of the
work. Use [the ranking](ranking.md) to pick the workloads that are safe to move.

### Disaggregation risk

The command sorts the workloads into three tiers. A Low-risk workload is safe to
move to the cloud. A High-risk workload reads a lot of data over the network, so
it stays on premises. See [the ranking](ranking.md) for the score and the tiers.

The summary counts every execution. The ranking groups the runs that repeat into
patterns, so the counts and the totals in the two commands do not match. Use the
summary for the whole workload. Use the ranking to see each pattern.

### Compute you can offload

- **Total CPU-time**: the compute the whole workload used.
- **Low-risk share**: the part of that compute the Low-risk workloads used. This
  is the compute you free on premises if you move the Low-risk workloads.

The share is often small. The workloads that read the most data also use the most
compute. They are High risk, so they stay on premises.

### Memory and node count (Impala)

- **Peak concurrent memory**: the highest memory demand across all queries that
  ran at the same time.
- **Memory per node**: the memory pool for one node. The command uses the largest
  single query it saw, plus 10%.
- **Smallest node count**: the fewest nodes that still hold the peak demand.

Headroom is spare memory. It protects against demand above the window you saw.
With 30% headroom, the cluster keeps 30% of its memory free at the peak. Pick a
higher headroom for a safer plan. Pick a lower headroom for a smaller cluster.

### Time breakdown

Both engines print this section. Every line has three indented lines under it that
split the time by disaggregation risk. Each risk share is a share of the wall-clock,
so the three lines add up to the line above them. They show how much of the time the
burstable (Low) workloads use and how much the WAN-bound (High) workloads use. The
High workloads often use the most time, even when they are fewer.

The wall-clock time of every application also splits three ways by phase. The three
parts add up to the wall-clock.

- **Resource wait**: the time the work waited for resources with nothing running.
  For Spark it is the time before the first executor arrived. For Impala it is the
  time the query sat in the admission queue. With enough capacity this time goes to
  zero. It is the time you win back by adding capacity.
- **Planning/startup**: the rest of the time outside execution. For Spark it is
  admission, planning, driver start-up, and the final result fetch. For Impala it is
  the compile and plan step, and the client fetch, when the coordinator holds the
  query open while the client reads the results. This time stays even with more
  capacity. For Impala the client fetch is the largest part, and more compute does
  not shrink it, because it waits on the client, not on the cluster.
- **Active processing**: the time the work was actually running.

The last line sends the resource wait to zero and shows the share of wall-clock
that stays. For Impala this share is large, because Impala is admitted almost at
once, so it has little wait to win back.

For Spark, a contention line follows. It shows the executor-slot wait that sits
inside active processing. It is not one of the three parts, because it overlaps
active processing. More capacity would shrink it too. Impala has no such line.

The parts are summed over every execution. Applications overlap in real time, so
the total is not the clock time on the wall. Read the shares, not the hours.

### Bursting potential

Both engines print this section. It shows the on-prem wall-clock you could reclaim by
bursting, in the ideal case. The **Reclaimable** total sums three contributors, shown
indented below it. The shares are shares of wall-clock, so the three add up to the
reclaimable share.

- **Move Low**: the whole Low workload runs in the cloud, so its full wall-clock
  leaves on premises. A Low-risk job reads little data, so it pays almost no network
  cost in the cloud.
- **Move Medium**: the same for the Medium workload. Test the Medium jobs first.
- **Relieve High**: the High workload stays on premises, because it reads too much
  data to run well over the network. But moving the Low and Medium workload frees
  capacity, so the High jobs that stay wait less. For Spark this is their resource
  wait (they get a first executor sooner) plus their contention (they wait less for a
  slot mid-run). For Impala this is their admission wait (they are admitted sooner)
  only, because Impala has no contention figure. Their active compute is not reclaimed.

Moving a workload takes its whole wall-clock, which already includes that workload's
own resource wait, and, for Spark, its contention. So those are counted once under
Move, and not again under Relieve. For Impala the Relieve High share is usually tiny,
because Impala admits queries almost at once, so there is little wait to free.

This is an upper bound. It assumes warm cloud capacity, and that the freed slots fully
relieve the High jobs. In reality the High jobs still compete with each other, and
cloud capacity has its own start-up time, so the real figure is lower.

See [the ranking](ranking.md) for the risk of each pattern.

### Executor use (Spark)

- **Peak executors**: the most executors one application used at the same time.
- **Executor-time**: the total time across all executors.
- **Core-time**: the executor-time multiplied by the cores per executor.

These values show the size of the compute the Spark workloads need.
