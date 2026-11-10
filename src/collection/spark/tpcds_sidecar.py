"""Extract what a TPC-DS run's metrics bundle knows and the event log does not.

Two things, both per query, and neither recoverable from the Spark event stream:

**The query name.** The benchmark driver issues all 99 queries from one call site, so
every ``SparkListenerSQLExecutionStart`` carries the same description
(``foreach at SparkSQLExecutor.java:89``). The names live only in the harness's
side-car, which records ``data.query_name`` and ``data.stage_id`` on every task
record. Written out at stage grain, because that is the grain the bundle records and
the grain that can be cross-checked against the log's own SQL-execution partition.

**The harness's own execution timer.** ``data.execution_timer.execution_time_in_ns``
is what the published dataset's ``exec_time_s`` was measured with (see
``spark-exec-predictor``'s ``stats_jobs.py``), so it is the only quantity that
compares to it like for like. It brackets more than the SQL execution - parse,
analysis and optimization, and any job the driver ran before posting
``SQLExecutionStart`` - and is consistently larger than the SQL span. On the 7.3.2
baseline the two differ by 91 s over 99 queries, and on individual queries by up to
8 s, so the choice is not cosmetic.

Keeping this here rather than in the parser is the point: a second benchmark needs
its own extractor next to this file and no change to ``model_parser``.

Usage:
    python -m src.collection.spark.tpcds_sidecar \
        --metrics results/onprem-baseline2/raw/metrics/dex_tpcds_sf1000-*.tar.gz \
        --output results/onprem-baseline2/sidecar.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
from collections import defaultdict
from pathlib import Path
from typing import Iterator

_QUERY_FILE = re.compile(r"(?:^|/)(query[0-9]+[ab]?)-\d+\.json$")


def _members(metrics: Path) -> Iterator[tuple[str, bytes]]:
    """Yield ``(name, content)`` for each per-query JSON, from a tarball or a directory."""
    if metrics.is_dir():
        for path in sorted(metrics.rglob("query*.json")):
            yield path.name, path.read_bytes()
        return
    with tarfile.open(metrics, "r:*") as tf:
        for member in tf.getmembers():
            if not member.isfile() or not _QUERY_FILE.search(member.name):
                continue
            fh = tf.extractfile(member)
            if fh is not None:
                yield member.name, fh.read()


def extract_sidecar(metrics: Path) -> tuple[dict[int, str], dict[str, float], list[str]]:
    """Read the bundle into ``({stage_id: query_name}, {query_name: seconds}, problems)``.

    A stage claimed by two query names is dropped and reported. That is a real
    contradiction in the source, and guessing one of the two would put the run in the
    wrong baseline group with nothing to show it happened. A query whose timer readings
    disagree is likewise reported rather than averaged.
    """
    claims: dict[int, set[str]] = defaultdict(set)
    timers: dict[str, set[float]] = defaultdict(set)
    files = 0
    for name, content in _members(metrics):
        files += 1
        try:
            records = json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"  SKIP {name}: {e}", file=sys.stderr)
            continue
        if not isinstance(records, list):
            continue
        for record in records:
            data = (record or {}).get("data") or {}
            query = data.get("query_name")
            stage = data.get("stage_id")
            if query is not None and stage is not None:
                try:
                    claims[int(stage)].add(str(query))
                except (ValueError, TypeError):
                    pass
            timer = data.get("execution_timer") or {}
            nanos = timer.get("execution_time_in_ns")
            timer_query = timer.get("query_name") or query
            if nanos and timer_query is not None:
                try:
                    timers[str(timer_query)].add(round(float(nanos) / 1e9, 6))
                except (ValueError, TypeError):
                    pass

    problems: list[str] = []
    if not files:
        problems.append(f"no query*.json found under {metrics}")

    labels: dict[int, str] = {}
    for stage, names in claims.items():
        if len(names) == 1:
            labels[stage] = next(iter(names))
        else:
            problems.append(
                f"stage {stage} claimed by {len(names)} queries: {', '.join(sorted(names))}")

    exec_times: dict[str, float] = {}
    for query, values in timers.items():
        if len(values) == 1:
            exec_times[query] = next(iter(values))
        else:
            problems.append(
                f"{query}: {len(values)} disagreeing timer readings "
                f"({', '.join(f'{v:.3f}' for v in sorted(values))} s)")
    return labels, exec_times, problems


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract the query names and harness timings from a TPC-DS metrics bundle")
    parser.add_argument("--metrics", type=Path, required=True,
                        help="The *-metrics.tar.gz bundle, or an unpacked directory")
    parser.add_argument("--output", type=Path, required=True,
                        help="Where to write the side-car JSON")
    args = parser.parse_args()

    if not args.metrics.exists():
        sys.exit(f"Error: metrics bundle not found: {args.metrics}")

    labels, exec_times, problems = extract_sidecar(args.metrics)
    for problem in problems:
        print(f"  WARN {problem}", file=sys.stderr)
    if not labels:
        sys.exit("Error: no stage labels found; is this a TPC-DS metrics bundle?")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump({
            "by_stage": {str(k): v for k, v in sorted(labels.items())},
            "exec_time_s": {k: exec_times[k] for k in sorted(exec_times)},
        }, f, indent=2)

    print(f"Wrote {len(labels)} stage labels over {len(set(labels.values()))} queries "
          f"and {len(exec_times)} harness timings to {args.output}", file=sys.stderr)
    missing = sorted(set(labels.values()) - set(exec_times))
    if missing:
        print(f"Note: no harness timing for {len(missing)} named quer(ies): "
              f"{', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}. "
              f"Their harness_exec_time_s will be null.", file=sys.stderr)


if __name__ == "__main__":
    main()
