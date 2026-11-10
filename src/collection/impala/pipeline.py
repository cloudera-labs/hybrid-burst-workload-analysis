"""Process raw Cloudera Manager Impala query exports into a compact summary JSON.

The raw exports under ``data/raw/impala`` (one combined ``impala_<from>Z_<to>Z.json`` per
fetch window) are large — millions of records, ~GBs — because every query carries dozens of
string attributes the analysis never reads. This step consolidates them into a single
``data/processed/impala/impala_summary.json`` that keeps the **same nested record shape** as
the raw export (so every downstream consumer — disaggregation_ranking and workload_summary —
reads it unchanged) while:

  * keeping only ``queryState == "FINISHED"`` queries with ``durationMillis > 0``;
  * deduplicating by ``queryId`` across all input files;
  * trimming ``attributes`` to the numeric keys the rankers actually use
    (``parser._NUMERIC_ATTRS``), dropping non-numeric noise (session_id, network_address,
    file_formats, pool, query_status, …).

This mirrors the Spark side, where spark_log_pipeline turns raw event-log zips into
measurement_summary.json.

Usage:
    python -m src.collection.impala.pipeline --input data/raw/impala --output data/processed/impala
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .parser import _NUMERIC_ATTRS

# Top-level scalar fields preserved verbatim (the raw CM query shape downstream expects).
_TOP_LEVEL_KEYS = (
    "queryId", "queryState", "durationMillis",
    "startTime", "endTime", "statement", "queryType",
)

# Output filename — matches the ``impala_*.json`` glob every consumer already uses.
OUTPUT_FILENAME = "impala_summary.json"


def _slim_record(q: dict) -> dict:
    """Reduce a raw CM query dict to the preserved top-level keys + numeric attributes."""
    slim = {k: q.get(k) for k in _TOP_LEVEL_KEYS if k in q}
    attrs = q.get("attributes", {})
    slim["attributes"] = {k: attrs[k] for k in _NUMERIC_ATTRS if k in attrs}
    return slim


def process_queries(queries: list[dict], seen: set[str]) -> tuple[list[dict], int, int]:
    """Filter to FINISHED, dedup by queryId (using shared ``seen``), and slim each record.

    Returns (kept_records, skipped_non_finished, dropped_duplicates).
    """
    kept: list[dict] = []
    skipped = 0
    dropped = 0
    for q in queries:
        if q.get("queryState") != "FINISHED" or (q.get("durationMillis") or 0) <= 0:
            skipped += 1
            continue
        qid = q.get("queryId")
        if qid is not None:
            if qid in seen:
                dropped += 1
                continue
            seen.add(qid)
        kept.append(_slim_record(q))
    return kept, skipped, dropped


def process_all(input_dir: Path) -> list[dict]:
    """Process every impala_*.json in input_dir into a deduped, slimmed record list."""
    files = sorted(input_dir.glob("impala_*.json"))
    if not files:
        sys.exit(f"Error: no impala_*.json files found in {input_dir}")

    print(f"Processing {len(files)} raw export(s) from {input_dir}", file=sys.stderr)

    records: list[dict] = []
    seen: set[str] = set()
    total_in = 0
    total_skipped = 0
    total_dropped = 0
    for i, f in enumerate(files, 1):
        with open(f) as fh:
            batch = json.load(fh)
        total_in += len(batch)
        kept, skipped, dropped = process_queries(batch, seen)
        records.extend(kept)
        total_skipped += skipped
        total_dropped += dropped
        print(f"  [{i}/{len(files)}] {f.name}: {len(batch)} in → {len(kept)} kept "
              f"({skipped} non-finished, {dropped} dup)", file=sys.stderr)

    print(f"Total: {total_in} raw → {len(records)} kept "
          f"({total_skipped} non-finished, {total_dropped} duplicates)", file=sys.stderr)
    return records


def write_output(records: list[dict], output_dir: Path) -> Path:
    """Write impala_summary.json into output_dir. Returns the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / OUTPUT_FILENAME
    with open(out_path, "w") as f:
        json.dump(records, f)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process raw Impala CM exports into a compact summary JSON"
    )
    parser.add_argument("--input", type=Path, default=Path("data/raw/impala"),
                        help="Directory containing raw impala_*.json exports")
    parser.add_argument("--output", type=Path, default=Path("data/processed/impala"),
                        help="Output dir for impala_summary.json")
    args = parser.parse_args()

    if not args.input.is_dir():
        sys.exit(f"Error: input directory not found: {args.input}")

    records = process_all(args.input)
    out_path = write_output(records, args.output)
    print(f"Wrote {len(records)} queries to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
