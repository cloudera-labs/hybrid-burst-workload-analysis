# Data pipeline

The analysis reads files from a `data/` folder. You build these files in two
steps: download the raw data, then process it into a small summary.

The `data/` folder is not in git. You build it on your own machine.

`ENGINE` selects the workload: `impala` or `spark`. Spark is the default.

## Step 1: download the raw data

```bash
make download ENGINE=impala            # Impala query history from the Cloudera Manager API
make download ENGINE=spark             # Spark event log zips from the History Server
```

The command downloads the last 7 days. Change the window with `DAYS`.

```bash
make download ENGINE=impala DAYS=14    # the last 14 days
```

The command skips the files that are already present. Run it again to get the
files that failed. Set the connection values in `.env` first.

Output:

- Impala → `data/raw/impala/impala_<from>Z_<to>Z.json`
- Spark → `data/raw/spark/*.log.zip`

## Step 2: process the raw data

```bash
make process ENGINE=impala   # data/raw/impala/*.json     → data/processed/impala/impala_summary.json
make process ENGINE=spark    # data/raw/spark/*.log.zip   → data/processed/spark/measurement_summary.json
```

The command always processes every downloaded file. It makes the files small. It
keeps only the fields the analysis reads.

For Spark, the process step also writes `stage_measurement_summary.json` next to
`measurement_summary.json`. It holds the per-stage detail. The ranking and
summary commands read `measurement_summary.json`; the stage file is there for
callers that need the stage-level numbers.

## Step 3: analyze

The analysis commands read `data/processed/<engine>`.

```bash
make ranking ENGINE=impala
make summary ENGINE=impala
```

See [Ranking](ranking.md) and [Summary](summary.md).

## The folder layout

```
data/
├── raw/
│   ├── impala/    # downloaded query history
│   └── spark/     # downloaded event log zips
└── processed/
    ├── impala/    # impala_summary.json
    └── spark/     # measurement_summary.json + stage_measurement_summary.json
```
