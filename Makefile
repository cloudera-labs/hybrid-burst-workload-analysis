.PHONY: install download process table-access ranking summary tpcds-sidecar model-features baseline-comparison stage-match plan-divergence verify-aggregation group-stability tpcds-aggregation error-sensitivity compress-ranking extract-ranking test help

# Load the .env file if it exists, and pass the values to every command.
ifneq (,$(wildcard .env))
include .env
export
endif

PYTHON := .venv/bin/python
PIP    := .venv/bin/pip

# ENGINE selects the workload: impala or spark. Spark is the default.
# DAYS sets how many past days download gets. WORKERS sets the parallel threads.
# DATA_ROOT is where the artifacts live. It defaults to this repository's own
# data/ so a standalone checkout behaves exactly as before, but a campaign
# points it at its own results directory instead - the artifacts belong to the
# campaign that produced them, not to this tool.
#   make download ENGINE=spark DATA_ROOT=/path/to/campaign/results/raw
DATA_ROOT ?= data
# model-features writes derived, committable output; a campaign separates it
# from the raw tier, so it has its own root.
MODEL_FEATURES_ROOT ?= $(DATA_ROOT)/model_features

ENGINE  ?= spark
DAYS    ?= 7
WORKERS ?= 10

# SPLIT is model-features' unit of analysis. "none" is one row per application,
# which is right for production, where an application is a query. "sql" is one row
# per SQL execution, which is what a benchmark harness needs: the TPC-DS driver runs
# all 99 queries inside a single application, so without it the whole run collapses
# into one row and the per-query structure the model calibrates on is lost.
# SIDECAR carries what the harness knows and the event log does not: the query name
# of each unit, and the harness's own per-query timer. Without it the units are keyed
# <app_id>#exec<n>, cannot be matched to a baseline, and harness_exec_time_s is null.
# Build it with the tpcds-sidecar target.
SPLIT   ?= none
SIDECAR ?=
# CONSTANTS is a JSON file holding the model's deployment and campaign constants.
# Prefer it over the SPARK_* environment variables: under GNU Make 3.81 a variable
# exported with the export directive is not visible to $(shell ...), so constants
# passed that way expand to nothing and every row is written null. A path is an
# ordinary Make variable and has no such problem.
CONSTANTS ?=

# ── Setup ─────────────────────────────────────────────────────────────────────

install:
	python3 -m venv .venv
	$(PIP) install -r requirements.txt

# ── Download raw data ─────────────────────────────────────────────────────────────
# Download the last DAYS days of raw data for ENGINE into data/raw/<engine>.
# The command skips the files that are already present. Run it again to get the
# files that failed. Set the connection values in .env first.
#   make download                       # Spark, the last 7 days
#   make download ENGINE=impala         # Impala, the last 7 days
#   make download ENGINE=impala DAYS=14 # Impala, the last 14 days

# NAME_CONTAINS keeps only the applications whose Spark app name contains it.
# A date window cannot separate one run from another: everything still inside the
# History Server's retention comes back, and model-features stamps ONE set of
# network constants onto every log in the directory it reads. Pass the run label.
NAME_CONTAINS ?=

# MIN_DATE keeps only the applications that ENDED after it (ISO-8601 GMT, e.g.
# 2026-09-09T02:11:00.000GMT). It is the second half of the filter, not an
# alternative to the first: the label separates one run from another, and this
# separates one ATTEMPT from another WITHIN a label. A run that failed and was
# re-run carries the same name as its retry - only the timestamp differs - so
# without a floor both attempts land in one directory and every downstream count
# is the sum of two runs.
#
# It filters on END time and on completed applications, so an attempt that is
# still running is not excluded by it and can finish above the floor. Kill a
# failed attempt before re-running rather than relying on this alone.
MIN_DATE ?=

# --name-contains is passed with an EQUALS, not a space, and that is load
# bearing. Callers filter by run label, and the natural filter is the label
# with its surrounding dashes - "-onprem-baseline-" - so the value starts with
# a dash. argparse reads a space-separated value that starts with "-" as
# another OPTION, and the run dies with "expected one argument" before a single
# log is fetched. The equals form has no such ambiguity.
download:
ifeq ($(ENGINE),impala)
	$(PYTHON) -m src.collection.impala.fetcher --days $(DAYS) --output $(DATA_ROOT)/raw/impala --workers $(WORKERS)
else
	$(PYTHON) -m src.collection.spark.downloader --days $(DAYS) --output $(DATA_ROOT)/raw/spark --workers $(WORKERS) $(if $(NAME_CONTAINS),--name-contains='$(NAME_CONTAINS)') $(if $(MIN_DATE),--min-date '$(MIN_DATE)')
endif

# ── Process raw data ──────────────────────────────────────────────────────────────
# Turn all the raw data for ENGINE into a small summary in data/processed/<engine>.
# The command always processes every downloaded file.
#   make process                # Spark
#   make process ENGINE=impala  # Impala

process:
ifeq ($(ENGINE),impala)
	$(PYTHON) -m src.collection.impala.pipeline --input $(DATA_ROOT)/raw/impala --output $(DATA_ROOT)/processed/impala
else
	$(PYTHON) -m src.collection.spark.pipeline --input $(DATA_ROOT)/raw/spark --output $(DATA_ROOT)/processed/spark
endif

# ── Table access ──────────────────────────────────────────────────────────────────
# Which tables each application reads and writes, and how many bytes of each, from
# the SQL plans in data/raw/spark → data/processed/spark/{table_access,
# table_access_apps,table_inventory}.json. Spark only, no network. It does not
# change match_id: event_parser's grouping keeps its own, narrower table pattern.
# A table's TOTAL size is not in an event log; scripts/hdfs_table_sizes.py measures
# it from table_inventory.json on a node with HDFS access (docs/table_access.md).
#   make table-access DATA_ROOT=/path/to/campaign/results/<run>
table-access:
	$(PYTHON) -m src.collection.spark.table_access --input $(DATA_ROOT)/raw/spark --output $(DATA_ROOT)/processed/spark --workers $(WORKERS)

# ── Model features (transport-aware schema) ────────────────────────────────────────
# Parse data/raw/spark/*.log.zip into the per-stage/per-job/per-graph feature JSON that
# the transport-aware-spark-model consumes →
# data/model_features/spark/{stage,job,graph}_features.json (they join on query_id).
# Spark only (the model is Spark-only); independent of process/ranking/summary. Every
# value is real Spark data except four deployment constants the event log cannot hold.
# Supply them so the output is model-ready by setting these in .env (all optional):
#   SPARK_MTU, SPARK_WINDOW_MAX_BYTES, SPARK_NETWORK_LATENCY_MS, SPARK_TSG_MAX_BW_GBS
# Left unset, those four columns are null and the command says which are missing.
#   make model-features                 # ENGINE defaults to spark
#   make model-features ENGINE=spark

# Extract what a TPC-DS run's metrics bundle knows and its event log does not: the
# query name of each unit, and the harness's own per-query execution timer.
#   make tpcds-sidecar METRICS=<...>-metrics.tar.gz SIDECAR=<run>/sidecar.json
tpcds-sidecar:
	@test -n "$(METRICS)" || { echo "Set METRICS=<path to the *-metrics.tar.gz bundle>"; exit 1; }
	@test -n "$(SIDECAR)" || { echo "Set SIDECAR=<path to write the side-car JSON>"; exit 1; }
	$(PYTHON) -m src.collection.spark.tpcds_sidecar --metrics $(METRICS) --output $(SIDECAR)

# ── Compare a collected TPC-DS baseline against the published one ─────────────
# Structure first, timing second: a structural column that disagrees means the
# per-query split is wrong, and the timings below it are not interpretable. Exits
# non-zero in that case, so it works as a campaign gate.
#   make baseline-comparison FEATURES=<results>/features/<label>/spark
PUBLISHED_DATA ?= $(HOME)/prj/akanto-cldr/transport-aware-spark-model/data

baseline-comparison:
	@test -n "$(FEATURES)" || { echo "Set FEATURES=<path to a model_features/spark dir>"; exit 1; }
	$(PYTHON) -m src.collection.analysis.baseline_comparison --features $(FEATURES) --published $(PUBLISHED_DATA)

# ── Pair the stages of two runs ───────────────────────────────────────────────
# The matched baseline stage is a model INPUT, so a wrong pair silently corrupts
# the calibration. Global, one-to-one and order-preserving; nothing is dropped,
# everything is labelled. ORDINAL_WEIGHT is the tie-break strength of stage
# position: leave it alone within a campaign, set it to 0 across runtimes, where
# position stops being evidence.
#   make stage-match LEFT=<baseline>/stage_features.json \
#                    RIGHT=<run>/stage_features.json [MATCHES=out.json]
ORDINAL_WEIGHT ?= 0.05

stage-match:
	@test -n "$(LEFT)"  || { echo "Set LEFT=<reference stage_features.json>"; exit 1; }
	@test -n "$(RIGHT)" || { echo "Set RIGHT=<stage_features.json to match>"; exit 1; }
	$(PYTHON) -m src.collection.analysis.stage_matcher --left $(LEFT) --right $(RIGHT) \
	  --ordinal-weight $(ORDINAL_WEIGHT) $(if $(MATCHES),--output $(MATCHES))

model-features:
ifeq ($(ENGINE),spark)
	$(PYTHON) -m src.collection.spark.model_pipeline --input $(DATA_ROOT)/raw/spark --output $(MODEL_FEATURES_ROOT)/spark --split $(SPLIT) $(if $(SIDECAR),--sidecar $(SIDECAR)) $(if $(CONSTANTS),--constants $(CONSTANTS))
else
	@echo "model-features is Spark only (the transport-aware model targets Spark). Use ENGINE=spark."; exit 1
endif

# ── Verify aggregation (graph reconstruction check) ─────────────────────────────────
# Check how well the group graph reconstructs the observed job time. Reads the model
# feature JSON that model-features writes (data/model_features/spark/{job,stage,graph}).
# For every run it feeds the real stage times through two aggregations — a plain
# cumulative sum and the dependency-aware grouping (the published model's own math) —
# and scores each against the observed exec_time_s. It prints a global table and a
# by-runtime table. It writes nothing. Spark only. Run model-features first.
#   make verify-aggregation             # ENGINE defaults to spark

verify-aggregation:
ifeq ($(ENGINE),spark)
	$(PYTHON) -m src.collection.analysis.verify_aggregation --input $(MODEL_FEATURES_ROOT)/spark
else
	@echo "verify-aggregation is Spark only (the transport-aware model targets Spark). Use ENGINE=spark."; exit 1
endif

# ── Group stability (does concurrency structure transfer between runs?) ─────────────
# The overlap grouping in verify-aggregation reads the timeline of the run it scores,
# so it is a ceiling rather than a predictor. This asks whether that grouping can be
# taken from an earlier execution of the same application instead. It builds families
# of repeated runs, scores each run with an earlier run's groups, and reports how much
# of the DAG-to-overlap gap the transfer recovers. Reads the same model feature JSON.
# It writes nothing. Spark only. Run model-features first.
#   make group-stability                # ENGINE defaults to spark

group-stability:
ifeq ($(ENGINE),spark)
	$(PYTHON) -m src.collection.analysis.group_stability --input $(MODEL_FEATURES_ROOT)/spark
else
	@echo "group-stability is Spark only (the transport-aware model targets Spark). Use ENGINE=spark."; exit 1
endif

# ── TPC-DS aggregation tables ──────────────────────────────────────────────────────
# Re-derive every TPC-DS table the aggregation study reports: the arms on the
# benchmark, the two saturated-case forms and how often they diverge, signed error,
# stage-graph sparsity against the production trace, the arms inside each interconnect
# configuration, and the transfer-share dilution control. Reads the dataset released
# with the transport-aware model; TPCDS_DIR points at that repo's data directory and
# is never written to. It writes nothing. Run model-features first for the sparsity row.
#   make tpcds-aggregation
#   make tpcds-aggregation TPCDS_DIR=/path/to/transport-aware-spark-model/data

TPCDS_DIR ?= $(HOME)/prj/akanto-cldr/transport-aware-spark-model/data

tpcds-aggregation:
	$(PYTHON) -m src.collection.analysis.tpcds_aggregation --tpcds-dir $(TPCDS_DIR)

# ── Error decomposition (which layer produces job-level error?) ────────────────────
# Job-level error comes from two layers, and which dominates has not been measured.
# This splits it exactly: e_agg is the aggregation's error with perfect stage times,
# e_prop is the stage model's error propagated through the aggregation, and the two
# sum to the total by construction. It also reports their correlation, so "do the
# layers cancel" is a measured covariance rather than an assumption. A third section
# scores end to end by stage-error regime; it is descriptive and confounded by query
# shape, and prints shape statistics so that is visible. Reads the same released
# dataset as tpcds-aggregation and writes nothing.
#   make error-sensitivity

error-sensitivity:
	$(PYTHON) -m src.collection.analysis.error_sensitivity --tpcds-dir $(TPCDS_DIR)

# ── Plan divergence (how far is the executed plan from the submitted one?) ────────
# A predictor that runs before the query can only read the plan in SQLExecutionStart.
# AQE then rewrites that plan every time a shuffle materialises, so the submitted plan
# is not what runs, and the size of that gap is a ceiling on any submission-time
# prediction. This counts Exchange nodes in the submitted plan against the last
# re-planned one, per query and per cell. Total node counts are NOT comparable and are
# not used: read docs/plan_divergence.md before changing what it counts. Reads event
# logs only and writes nothing unless DRIFT is set. Names come from each cell's
# sidecar.json, as everywhere else.
#   make plan-divergence RESULTS=<campaign results dir>
#   make plan-divergence RESULTS=<dir> DRIFT=drift.json
#   make plan-divergence LOG=<run>/raw/spark/app.log.zip SIDECAR=<run>/sidecar.json \
#                        LABEL=onprem-baseline

plan-divergence:
	@test -n "$(RESULTS)$(LOG)" || { echo "Set RESULTS=<campaign results dir> or LOG=<event log zip>"; exit 1; }
	$(PYTHON) -m src.collection.analysis.plan_divergence \
	  $(if $(RESULTS),--results $(RESULTS)) \
	  $(if $(LOG),--log $(LOG)) $(if $(SIDECAR),--sidecar $(SIDECAR)) \
	  $(if $(LABEL),--label $(LABEL)) $(if $(DRIFT),--output $(DRIFT))

# ── Ranking ───────────────────────────────────────────────────────────────────────
# Rank the workloads by how much disaggregation affects them, and group the runs
# that repeat. The command reads the processed data. Run fetch and process first.
# Add JSON=1 to also save the result to data/ranking/<engine>_ranking.json.
#   make ranking                # Spark
#   make ranking ENGINE=impala  # Impala
#   make ranking ENGINE=impala JSON=1

ranking:
	$(PYTHON) -m src.collection.analysis.ranking --engine $(ENGINE)$(if $(JSON), --save-json,)

# ── Summary ─────────────────────────────────────────────────────────────────────
# Print the workload summary: data volume, network rate, risk mix, and the compute
# you can move to the cloud. Impala adds the memory and node count. Spark adds the
# executor use. The command reads the processed data. Run fetch and process first.
#   make summary                # Spark
#   make summary ENGINE=impala  # Impala

summary:
	$(PYTHON) -m src.collection.analysis.summary --engine $(ENGINE)

# ── Ranking snapshot ─────────────────────────────────────────────────────────────
# The working JSON is in data/ranking (git ignores it; you regenerate it).
# compress bundles it into snapshots/ranking.tar.gz. Share that file. extract puts
# the JSON back into the working directory.

RANKING_DIR := $(DATA_ROOT)/ranking
RANKING_ARCHIVE := snapshots/ranking.tar.gz

compress-ranking:
	@ls $(RANKING_DIR)/*_ranking.json >/dev/null 2>&1 || \
		{ echo "No ranking JSON in $(RANKING_DIR); run 'make ranking ENGINE=<engine> JSON=1' first"; exit 1; }
	@mkdir -p $(dir $(RANKING_ARCHIVE))
	tar -czf $(RANKING_ARCHIVE) -C $(RANKING_DIR) $$(cd $(RANKING_DIR) && ls *_ranking.json)
	@echo "Compressed $$(cd $(RANKING_DIR) && ls *_ranking.json | wc -l | tr -d ' ') file(s) → $(RANKING_ARCHIVE) ($$(du -h $(RANKING_ARCHIVE) | cut -f1))."

extract-ranking:
	@test -f $(RANKING_ARCHIVE) || { echo "No archive at $(RANKING_ARCHIVE)"; exit 1; }
	@mkdir -p $(RANKING_DIR)
	tar -xzf $(RANKING_ARCHIVE) -C $(RANKING_DIR)
	@echo "Extracted → $(RANKING_DIR)/"

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	$(PYTHON) -m pytest tests/ -v; status=$$?; [ $$status -eq 0 ] || [ $$status -eq 5 ]

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo "Targets (set ENGINE=impala|spark; spark is the default):"
	@echo "  install              create the .venv and install the dependencies"
	@echo "  download             download the last DAYS days of raw data → \$$(DATA_ROOT)/raw/<engine>"
	@echo "                       DATA_ROOT defaults to data/; a campaign overrides it"
	@echo "                       DAYS defaults to 7; set WORKERS for more threads"
	@echo "  process              make a small summary from data/raw/<engine> → data/processed/<engine>"
	@echo "  table-access         which tables each Spark app reads and writes → data/processed/spark"
	@echo "  tpcds-sidecar        extract the query names and the harness's own per-query"
	@echo "                       timer from a TPC-DS metrics bundle; the event log has neither"
	@echo "                       METRICS=<*-metrics.tar.gz> SIDECAR=<sidecar.json to write>"
	@echo "  model-features       parse data/raw/spark → transport-aware-model feature JSON"
	@echo "                       (data/model_features/spark/{stage,job,graph}_features.json); Spark only"
	@echo "                       SPLIT=none|sql selects the unit: one row per application"
	@echo "                       (production) or one per SQL execution (a benchmark that runs"
	@echo "                       many queries in one application). SIDECAR names the units"
	@echo "                       and carries the harness timings."
	@echo "                       Pass the model constants in one file: --constants / CONSTANTS"
	@echo "  stage-match          pair the stages of two runs (LEFT=reference, RIGHT=run)"
	@echo "                       one-to-one and order-preserving; nothing dropped, all labelled"
	@echo "                       ORDINAL_WEIGHT=0 across runtimes; MATCHES=<file> to save"
	@echo "  baseline-comparison  check a collected TPC-DS baseline against the published one"
	@echo "                       FEATURES=<results>/features/<label>/spark; non-zero on a"
	@echo "                       structural mismatch, so it works as a gate"
	@echo "  verify-aggregation   check how well the group graph reconstructs observed job time"
	@echo "                       (reads the model-features JSON; prints two tables); Spark only"
	@echo "  group-stability      test whether a run's stage concurrency structure transfers"
	@echo "                       from an earlier execution of the same application; Spark only"
	@echo "  tpcds-aggregation    re-derive the TPC-DS tables (arms, saturated-case forms,"
	@echo "                       sparsity, per-configuration invariance, dilution control)"
	@echo "                       set TPCDS_DIR to the transport-aware-spark-model data dir"
	@echo "  error-sensitivity    split job-level error into its aggregation and stage-model"
	@echo "                       terms exactly, and measure whether the two cancel"
	@echo "  plan-divergence      measure the gap between the submitted plan and the plan"
	@echo "                       that ran, per query and per cell (Exchange nodes only)"
	@echo "                       RESULTS=<campaign results dir>, or LOG= with SIDECAR="
	@echo "                       and LABEL=; DRIFT=<file> to save the per-query rows"
	@echo "  ranking              rank the processed workloads by disaggregation sensitivity"
	@echo "                       add JSON=1 to save data/ranking/<engine>_ranking.json"
	@echo "  summary              print the workload summary from the processed data"
	@echo "  compress-ranking     bundle data/ranking/*_ranking.json → snapshots/ranking.tar.gz"
	@echo "  extract-ranking      restore the ranking JSON from snapshots/ranking.tar.gz"
	@echo "  test                 run the tests"
	@echo ""
	@echo "A full run:  make download ENGINE=impala  →  make process ENGINE=impala  →"
	@echo "             make ranking ENGINE=impala  →  make summary ENGINE=impala"
	@echo "Set the connection values in .env first. Copy .env.example to .env and fill it in."
