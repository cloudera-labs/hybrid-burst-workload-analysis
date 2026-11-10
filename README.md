# Hybrid Burst Workload Analysis

This repository helps you plan a move of compute to the cloud. It reads the
Impala and Spark workload history from your cluster. It ranks the workloads by
how much a compute-and-storage split affects each one. It also prints a summary:
the data volume, the network rate you need, the risk mix, and the compute you can
move. For Impala it also prints the memory and the smallest node count.

The workloads that are safe to move are the ones that read little data. The
workloads that read a lot of data are sensitive to the network, so they stay on
premises.

## Disclaimer

Please read the following before proceeding.  Cloudera, Inc. (“Cloudera”) makes this software available as an open source tool for the convenience of its users.  Although Cloudera expects that the tool will help users analyze workloads for potential cloud-onboarding, Cloudera makes the tool available “as is” and without any warranty or support.  By downloading and using this tool, you acknowledge the foregoing statement and agree that Cloudera is not responsible or liable in any way for your use of the tool.

## Requirements

- Python 3.10 or later.
- Read access to the Cloudera Manager API (for Impala).
- Read access to the Spark History Server (for Spark).

## Setup

1. Install the dependencies.

   ```bash
   make install
   ```

2. Copy the settings file and fill it in.

   ```bash
   cp .env.example .env
   ```

   Open `.env` and set the values for your cluster:

   | Setting | What to enter |
   |---------|---------------|
   | `CM_BASE` | The Cloudera Manager API URL, for example `https://host:7183/api/v54`. |
   | `CM_CLUSTER` | The cluster name in Cloudera Manager. |
   | `CM_USER`, `CM_PASSWORD` | The user name and the password for the API. |
   | `SPARK_HISTORY_URL` | The Spark History Server root, for example `https://host:18489`. |

   The tools use basic authentication and send GET requests only.

   Do not commit `.env`. Do not share a password.

## Commands

`ENGINE` selects the workload: `impala` or `spark`. Spark is the default.

First, download and process the history. See [docs/pipeline.md](docs/pipeline.md).
The `download` command gets the last 7 days. Change the window with `DAYS`.

```bash
make download ENGINE=spark    # download the last 7 days → data/raw/spark
make process ENGINE=spark     # make a small summary → data/processed/spark
```

Then run an analysis on the processed data.

```bash
make ranking ENGINE=spark     # rank the workloads (prints a table)
make summary ENGINE=spark     # print the workload overview
```

| Command | What it does |
|---------|--------------|
| `make download ENGINE=impala\|spark` | Download raw history into `data/raw/<engine>`. |
| `make process ENGINE=impala\|spark` | Build a small summary in `data/processed/<engine>`. |
| `make table-access` | Spark: which tables each app reads and writes, with bytes, in `data/processed/spark`. See `docs/table_access.md`. |
| `make ranking ENGINE=impala\|spark` | Rank the workloads by disaggregation sensitivity. Prints a table. |
| `make summary ENGINE=impala\|spark` | Show the data volume, network rate, risk mix, and the compute you can move. |

Run `make help` for the full list.

### Options

Pass these on the `make` command line.

| Option | Default | Applies to | What it does |
|--------|---------|------------|--------------|
| `ENGINE=impala\|spark` | `spark` | all | Select Impala or Spark. |
| `DAYS=N` | `7` | `download` | Download the last N days of history. |
| `WORKERS=N` | `10` | `download` | Use N parallel download threads. |
| `JSON=1` | off | `ranking` | Also save the ranking to `data/ranking/<engine>_ranking.json`. The text table still prints. |

Examples:

```bash
make download ENGINE=spark DAYS=30          # the last 30 days
make download ENGINE=impala DAYS=14 WORKERS=20
make ranking ENGINE=spark JSON=1            # print the table and save JSON
make ranking ENGINE=impala JSON=1
```

To share a saved ranking, run `make compress-ranking`. That bundles
`data/ranking/*_ranking.json` into `snapshots/ranking.tar.gz`.

## Documentation

The [docs](docs/README.md) folder explains what each command prints and how to
read it.

## License

This project uses the Apache License 2.0. See [LICENSE](LICENSE). The
[NOTICE](NOTICE) file lists the third-party packages and their licenses.
