# Copyright 2026 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Parse Spark event log zips into transport-aware-model feature JSON.

Reads ``data/raw/spark/*.log.zip`` (the download step's output) and writes, per
run, the stage, job, and graph feature records the ``transport-aware-spark-model``
consumes:

    data/model_features/spark/stage_features.json   one row per stage
    data/model_features/spark/job_features.json     one row per application
    data/model_features/spark/graph_features.json   one row per application (the
        stage-dependency group graph; joins to the other two on ``query_id``)

One event log is one *application*, and in production that is also one query, so
the default emits one job row per log. A benchmark harness breaks that assumption:
the TPC-DS driver runs all 99 queries inside a single application. ``--split sql``
cuts the application into one unit per SQL execution, which is the model's row.
Query names are not in the event log, and neither is the harness's own execution
timer; supply both with ``--sidecar`` (see ``tpcds_sidecar``), or the units are
keyed ``<app_id>#exec<n>`` and ``harness_exec_time_s`` is null.

Four model inputs are OS/network constants that no event log contains
(``window_max_bytes``, ``mtu``, ``network_latency_ms``, ``tsg_max_bw_gbs``).
Supply them with the flags below (or the matching env vars) to stamp every row so
the output is model-ready; leave them off and those columns are ``null``. The
command prints which ones were left unset.

Usage:
    python -m src.collection.spark.model_pipeline --input data/raw/spark \
        --output data/model_features/spark \
        --mtu 9001 --window-max-bytes 134217728 \
        --network-latency-ms 10 --tsg-max-bw-gbs 25

    # a TPC-DS run: 99 query rows out of one application
    python -m src.collection.spark.model_pipeline --input results/onprem/raw/spark \
        --output results/features/onprem/spark \
        --split sql --sidecar results/onprem/sidecar.json ...
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from .model_parser import (
    EnvConstants,
    GraphFeature,
    JobFeature,
    StageFeature,
    _State,
    build_graph,
    build_results,
    process_event,
)
from .run_splitter import SPLIT_NONE, SPLITTERS, Sidecar, split_state


def parse_single_log(
    zip_path: Path, env: EnvConstants, split: str = SPLIT_NONE,
    sidecar: Optional[Sidecar] = None,
) -> tuple[list[JobFeature], list[StageFeature], list[GraphFeature]]:
    """Parse one event log zip into job + stage + graph records (one pass over the log).

    Returns one job and one graph per *unit*: the whole application under the default
    ``none`` split, one per SQL execution under ``sql``.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            if not names:
                return [], [], []
            with zf.open(names[0]) as f:
                state = _State()
                for raw_line in f:
                    try:
                        event = json.loads(raw_line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if isinstance(event, dict):
                        process_event(event, state)

            jobs: list[JobFeature] = []
            stages: list[StageFeature] = []
            graphs: list[GraphFeature] = []
            for unit, sub in split_state(state, split, sidecar):
                job, unit_stages = build_results(sub, env, unit)
                if job is not None:
                    jobs.append(job)
                    stages.extend(unit_stages)
                graph = build_graph(sub, unit)
                if graph is not None:
                    graphs.append(graph)
            return jobs, stages, graphs
    except Exception as e:
        print(f"  SKIP {zip_path.name}: {e}", file=sys.stderr)
        return [], [], []


def parse_all_logs(input_dir: Path, env: EnvConstants, split: str = SPLIT_NONE,
                   sidecar: Optional[Sidecar] = None) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse all *.log.zip files in input_dir."""
    zip_files = sorted(input_dir.glob("*.log.zip"))
    if not zip_files:
        zip_files = sorted(input_dir.glob("*.zip"))

    job_records: list[dict] = []
    stage_records: list[dict] = []
    graph_records: list[dict] = []
    unnamed = 0
    untimed = 0
    total = len(zip_files)

    print(f"Parsing {total} event logs from {input_dir} (split={split})", file=sys.stderr)

    for i, zp in enumerate(zip_files, 1):
        jobs, stages, graphs = parse_single_log(zp, env, split, sidecar)
        job_records.extend(asdict(j) for j in jobs)
        stage_records.extend(asdict(s) for s in stages)
        graph_records.extend(asdict(g) for g in graphs)
        unnamed += sum(1 for j in jobs if "#exec" in j.query_id)
        untimed += sum(1 for j in jobs if j.harness_exec_time_s is None)
        if i % 10 == 0 or i == total:
            print(f"  Progress: {i}/{total} ({len(job_records)} jobs, "
                  f"{len(stage_records)} stages, {len(graph_records)} graphs)", file=sys.stderr)

    if split != SPLIT_NONE and unnamed:
        print(f"Note: {unnamed} of {len(job_records)} units have no query name and are "
              f"keyed <app_id>#exec<n>. They cannot be matched to a baseline run. "
              f"Pass --sidecar to name them.", file=sys.stderr)
    if split != SPLIT_NONE and untimed:
        print(f"Note: {untimed} of {len(job_records)} units have no harness timing "
              f"(harness_exec_time_s is null). exec_time_s is still real, but it is the "
              f"SQL-execution span, which is not how the published exec_time_s was "
              f"measured.", file=sys.stderr)
    if sidecar is not None and sidecar.conflicts:
        for c in sidecar.conflicts[:10]:
            print(f"  WARN side-car conflict, {c}", file=sys.stderr)

    return job_records, stage_records, graph_records


def write_output(job_records: list[dict], stage_records: list[dict],
                 graph_records: list[dict], output_dir: Path) -> None:
    """Write job_features.json, stage_features.json, and graph_features.json."""
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "job_features.json", "w") as f:
        json.dump(job_records, f, indent=2)
    with open(output_dir / "stage_features.json", "w") as f:
        json.dump(stage_records, f, indent=2)
    with open(output_dir / "graph_features.json", "w") as f:
        json.dump(graph_records, f, indent=2)


#: Every constant, with its CLI attribute, its environment variable, and its type.
_CONSTANTS: tuple[tuple[str, str, type], ...] = (
    ("window_max_bytes", "SPARK_WINDOW_MAX_BYTES", int),
    ("mtu", "SPARK_MTU", int),
    ("network_latency_ms", "SPARK_NETWORK_LATENCY_MS", float),
    ("tsg_max_bw_gbs", "SPARK_TSG_MAX_BW_GBS", float),
    ("data_size_gb", "SPARK_DATA_SIZE_GB", float),
    ("file_format", "SPARK_FILE_FORMAT", str),
    ("compression_codec", "SPARK_COMPRESSION_CODEC", str),
    ("num_workers", "SPARK_NUM_WORKERS", int),
    ("worker_disk_bw_gbps", "SPARK_WORKER_DISK_BW_GBPS", float),
    ("worker_disk_count", "SPARK_WORKER_DISK_COUNT", int),
    ("worker_disk_bw_per_volume_gbps", "SPARK_WORKER_DISK_BW_PER_VOLUME_GBPS", float),
    ("worker_net_bw_gbps", "SPARK_WORKER_NET_BW_GBPS", float),
    ("tsg_avg_bw_gbs", "SPARK_TSG_AVG_BW_GBS", float),
    ("tsg_transfer_gb", "SPARK_TSG_TRANSFER_GB", float),
)


def _resolve_env_constants(args: argparse.Namespace) -> EnvConstants:
    """Resolve every constant: explicit CLI flag, then --constants file, then env var.

    Explicit beats the file, and the file beats the ambient environment, so a campaign
    can keep one committed constants file per cell and still override a single value on
    the command line. A value that will not parse is reported and left unset - never
    silently coerced, because these are the experiment's independent variables.
    """
    from_file: dict = {}
    if getattr(args, "constants", None) is not None:
        from_file = json.loads(args.constants.read_text())
        if not isinstance(from_file, dict):
            sys.exit(f"Error: {args.constants} must hold a JSON object of constants")
        unknown = set(from_file) - {name for name, _, _ in _CONSTANTS}
        if unknown:
            print(f"  WARN {args.constants}: ignoring unknown key(s) "
                  f"{', '.join(sorted(unknown))}", file=sys.stderr)

    def _resolve(name: str, env_key: str, cast: type):
        cli = getattr(args, name, None)
        if cli is not None:
            return cli
        for source, raw in ((str(args.constants), from_file.get(name)),
                            (env_key, os.environ.get(env_key))):
            if raw is None or raw == "":
                continue
            try:
                return cast(raw)
            except (ValueError, TypeError):
                print(f"  WARN {source}: {name}={raw!r} is not a valid "
                      f"{cast.__name__}; treating as unset", file=sys.stderr)
                return None
        return None

    values = {name: _resolve(name, env_key, cast) for name, env_key, cast in _CONSTANTS}
    try:
        return EnvConstants(**values)
    except ValueError as e:
        sys.exit(f"Error: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Spark event log zips into transport-aware-model feature JSON")
    parser.add_argument("--input", type=Path, default=Path("data/raw/spark"),
                        help="Directory containing *.log.zip files")
    parser.add_argument("--output", type=Path, default=Path("data/model_features/spark"),
                        help="Output dir for the model feature JSON files")
    parser.add_argument("--window-max-bytes", type=int, default=None,
                        help="TCP max window in bytes (sysctl net.ipv4.tcp_rmem / net.core.rmem_max)")
    parser.add_argument("--mtu", type=int, default=None,
                        help="Network MTU in bytes (ip link show <iface>)")
    parser.add_argument("--network-latency-ms", type=float, default=None,
                        help="RTT to the cloud side in ms (ping), or the link spec")
    parser.add_argument("--tsg-max-bw-gbs", type=float, default=None,
                        help="WAN interconnect capacity in Gb/s")
    parser.add_argument("--data-size-gb", type=float, default=None,
                        help="Physical size of the dataset under test, in GB")
    parser.add_argument("--file-format", choices=("parquet", "orc"), default=None,
                        help="Columnar storage format of the dataset")
    parser.add_argument("--compression-codec",
                        choices=("uncompressed", "snappy", "gzip"), default=None,
                        help="Compression codec of the dataset")
    parser.add_argument("--num-workers", type=int, default=None,
                        help="Number of cluster worker nodes")
    parser.add_argument("--worker-disk-bw-gbps", type=float, default=None,
                        help="Per-worker disk bandwidth in Gb/s")
    parser.add_argument("--worker-net-bw-gbps", type=float, default=None,
                        help="Per-worker network bandwidth in Gb/s")
    parser.add_argument("--worker-disk-count", type=int, default=None,
                        help="Data volumes attached to each worker. With "
                             "--worker-disk-bw-per-volume-gbps it says which ceiling "
                             "binds; it does not redefine --worker-disk-bw-gbps")
    parser.add_argument("--worker-disk-bw-per-volume-gbps", type=float, default=None,
                        help="Throughput of ONE data volume in Gb/s. Provisioners "
                             "derive gp3 throughput from volume size, so this is a "
                             "provisioning fact, not a choice")
    parser.add_argument("--tsg-avg-bw-gbs", type=float, default=None,
                        help="Observed average throughput over the interconnect in Gb/s "
                             "(0.0 on an on-premises baseline)")
    parser.add_argument("--tsg-transfer-gb", type=float, default=None,
                        help="Total volume transferred over the interconnect in GB "
                             "(0.0 on an on-premises baseline)")
    parser.add_argument("--constants", type=Path, default=None,
                        help="JSON object holding any of the constants above, one file "
                             "per grid cell. A flag overrides the file; the file "
                             "overrides the SPARK_* environment variables.")
    parser.add_argument("--split", choices=SPLITTERS, default=SPLIT_NONE,
                        help="Unit of analysis: 'none' = one row per application "
                             "(production default); 'sql' = one row per SQL execution "
                             "(a benchmark that runs many queries in one application)")
    parser.add_argument("--sidecar", type=Path, default=None,
                        help="JSON holding what the harness knows and the event log does "
                             "not: query names ({\"by_stage\"} or {\"by_exec\"}) and its own "
                             "per-query timer ({\"exec_time_s\"}). Build it with "
                             "src/collection/spark/tpcds_sidecar.py")
    args = parser.parse_args()

    if not args.input.is_dir():
        sys.exit(f"Error: input directory not found: {args.input}")
    if args.constants is not None and not args.constants.is_file():
        sys.exit(f"Error: constants file not found: {args.constants}")

    sidecar = None
    if args.sidecar is not None:
        if not args.sidecar.is_file():
            sys.exit(f"Error: side-car file not found: {args.sidecar}")
        sidecar = Sidecar.from_dict(json.loads(args.sidecar.read_text()))
        if not sidecar:
            sys.exit(f"Error: side-car file is empty: {args.sidecar}")
        if args.split == SPLIT_NONE:
            print("Note: --sidecar has no effect with --split none (the unit is the "
                  "whole application, keyed by its app id).", file=sys.stderr)

    env = _resolve_env_constants(args)
    job_records, stage_records, graph_records = parse_all_logs(
        args.input, env, args.split, sidecar)
    write_output(job_records, stage_records, graph_records, args.output)

    print(f"Wrote {len(job_records)} jobs to {args.output / 'job_features.json'}",
          file=sys.stderr)
    print(f"Wrote {len(stage_records)} stages to {args.output / 'stage_features.json'}",
          file=sys.stderr)
    print(f"Wrote {len(graph_records)} graphs to {args.output / 'graph_features.json'}",
          file=sys.stderr)
    missing = env.missing()
    if missing:
        print(f"Note: transport constants left unset (null in every row): "
              f"{', '.join(missing)}. The stage model cannot run without them. "
              f"Pass them as flags, in --constants, or as env vars.", file=sys.stderr)
    missing_campaign = env.missing_campaign()
    if missing_campaign:
        print(f"Note: campaign constants left unset (null in every row): "
              f"{', '.join(missing_campaign)}. A run without them can be scored on its "
              f"own but not matched to a baseline run.", file=sys.stderr)


if __name__ == "__main__":
    main()
