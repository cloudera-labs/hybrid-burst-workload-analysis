# Ranking

## What the command does

The ranking command scores each workload by how much disaggregation affects it.
It sorts the workloads from least affected to most affected. The workload at
rank 1 is the safest to move to the cloud.

```bash
make ranking ENGINE=impala   # reads data/processed/impala
make ranking ENGINE=spark    # reads data/processed/spark
```

Run the download and process commands first. See [the data pipeline](pipeline.md).

## The score

Each workload gets a score from 0 to 300. The score is the sum of three parts.
Each part goes from 0 to 100.

| Part | What it measures | Reaches 100 at |
|------|------------------|----------------|
| Data volume | The total bytes read plus written. | 3 GB |
| Bandwidth | The bytes moved for each second the workload runs. | 1 Gbit/s |
| Files | The number of files or splits the workload opens. | 1000 files |

A high data volume uses more of the network over time. A high bandwidth fills
the network link. A high file count adds delay, because each file open waits for
the network round trip.

You can change the three limits on the command line: `--ref-data`, `--ref-gbps`,
and `--ref-files`.

## The risk tiers

The command puts each workload in one of three tiers.

| Tier | Rule | Meaning |
|------|------|---------|
| Low | Score is 100 or less, and no part reaches its limit. | Safe to move. |
| Medium | Score is more than 100, but no part reaches its limit. | Test it first. |
| High | One part or more reaches its limit. | Expect a slowdown. |

A workload is High risk if even one part reaches its limit. That workload runs at
the physical limit of that part.

## The "Why" label

The command adds a "Why" label to the Medium and High workloads. The label names
the part that matters most: **Data**, **BW** (bandwidth), or **Files**. A High
workload can hit more than one part, for example **Data+BW**.

A Low workload has no label. It is safe for any reason.

## The printed table

The command prints one row for each workload. The table is wide. The columns are:

| Column | Meaning |
|--------|---------|
| Rank | The position. Rank 1 has the lowest score. |
| Score | The total score, from 0 to 300. |
| Data% / BW% / File% | The three score parts. Each part goes from 0 to 100. |
| Duration | The wall-clock time the workload ran. |
| Exec | The active execution time. It leaves out the queue time and the idle time. |
| Overhead | Impala only. The wall-clock time not spent in execution: the admission queue wait, the compile and plan step, and the client fetch. The client fetch is usually the largest part. |
| Res wait | Spark only. The wall-clock time before the first executor arrived. Nothing runs in this time. With enough capacity it goes to zero. |
| Startup | Spark only. The rest of the overhead: admission, planning, driver start-up, and result fetch. It stays even with enough capacity. |
| Contention | Spark only. The time tasks waited for an executor slot. It sits inside Exec. |
| Read / Written | The bytes read and the bytes written. |
| Read Intensity / Write Intensity | The bytes moved for each active second. |
| CPU-s | The CPU time the workload used, in CPU-seconds. |
| CPU Intensity | The CPU-seconds used for each active second (the mean cores busy). |
| Exec a/pk | Spark only. The mean and the peak executor count. |
| Alloc | Spark only. The allocation mode: `dyn`, `static`, or `?`. |
| Exec size | Spark only. The cores and the memory for one executor. |
| Files | The number of files or splits the workload opens. |
| Runs | The number of runs in the group. It shows only when the runs are grouped. |
| Risk | Low, Medium, or High. |
| Why | The part that matters most. |
| Type | The statement kind, for example `query`, `sql`, or `rdd`. |
| Pattern / Name | The grouping pattern when grouped, or the workload name or SQL text. |

For Spark, the wall-clock splits three ways and the parts add up:
**Exec + Res wait + Startup = Duration**. Res wait is the time you win back with
more executors. Startup stays. For Impala the table shows one Overhead column
instead: **Exec + Overhead = Duration**. The Overhead holds the admission wait, the
planning, and the client fetch together. The summary command splits these for
Impala, the same way it splits Res wait and Startup for Spark.

The command groups the runs that repeat by default. It folds many runs of one
workload into one row. That row is a **Pattern**. For a grouped row, Duration,
Exec, Res wait, Startup, Read, Written, and CPU-s show the mean of the runs, and
the spread comes after a `±` sign. The Duration header shows a `~` to mark this.

## The capacity offload summary

The command prints a "Capacity Offload" summary when the data has CPU-time. Both
Impala and Spark provide it. It shows how much CPU time each tier uses.

```
── Capacity Offload ──
  Total CPU-time:    <n> CPU-sec
  Low risk:          <n> CPU-sec  (<p>%)
  Medium risk:       <n> CPU-sec  (<p>%)
  High risk:         <n> CPU-sec  (<p>%)
  Offloadable:       <p>% of on-prem compute can be freed by moving Low-risk jobs to cloud
```

Read the "Offloadable" line for the result. It tells you how much on-premises
compute you free if you move the Low-risk workloads to the cloud.

The offloadable part is often small. The workloads that read the most data also
use the most CPU. They are High risk, so they stay on premises.

## How to act

1. Start at rank 1. Move the Low-risk workloads first.
2. Test each workload on premises and against remote storage. Compare the time.
3. Stop when the combined bandwidth of the workloads that run together reaches
   your network link limit. See [the summary](summary.md) for the network rate.

## JSON output

Add `JSON=1` to save the result as a file.

```bash
make ranking ENGINE=impala JSON=1   # → data/ranking/impala_ranking.json
```

The `*_bps` fields hold bits per second. This matches the network link unit
(Gbit/s). The `bytes_*` fields hold raw bytes. The times are in seconds.
