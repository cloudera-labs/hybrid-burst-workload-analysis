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

"""Orchestrate parsing of Spark event log zips into summary JSON files.

Usage:
    python -m src.collection.spark.pipeline --input data/raw/spark --output data/processed/spark
"""

from __future__ import annotations

import json
import sys
import zipfile
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from .event_parser import (
    JobSummary,
    StageSummary,
    _ParserState,
    build_results,
    process_event,
)


def parse_single_log(zip_path: Path) -> tuple[Optional[JobSummary], list[StageSummary]]:
    """Parse one event log zip into job + stage summaries."""
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            if not names:
                return None, []
            with zf.open(names[0]) as f:
                state = _ParserState()
                for raw_line in f:
                    try:
                        event = json.loads(raw_line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if isinstance(event, dict):
                        process_event(event, state)
                if not state.app_id:
                    return None, []
                return build_results(state)
    except Exception as e:
        print(f"  SKIP {zip_path.name}: {e}", file=sys.stderr)
        return None, []


def parse_all_logs(input_dir: Path) -> tuple[list[dict], list[dict]]:
    """Parse all *.log.zip files in input_dir."""
    zip_files = sorted(input_dir.glob("*.log.zip"))
    if not zip_files:
        zip_files = sorted(input_dir.glob("*.zip"))

    job_records: list[dict] = []
    stage_records: list[dict] = []
    total = len(zip_files)

    print(f"Parsing {total} event logs from {input_dir}", file=sys.stderr)

    for i, zp in enumerate(zip_files, 1):
        job, stages = parse_single_log(zp)
        if job is not None:
            job_records.append(asdict(job))
            stage_records.extend(asdict(s) for s in stages)
        if i % 10 == 0 or i == total:
            print(f"  Progress: {i}/{total} ({len(job_records)} jobs, {len(stage_records)} stages)", file=sys.stderr)

    return job_records, stage_records


def write_output(job_records: list[dict], stage_records: list[dict], output_dir: Path) -> None:
    """Write measurement_summary.json (jobs) and stage_measurement_summary.json (stages).

    The ranking and summary commands read measurement_summary.json. The per-stage
    file is written alongside it for callers that need stage-level detail.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    job_path = output_dir / "measurement_summary.json"
    with open(job_path, "w") as f:
        json.dump(job_records, f, indent=2)

    stage_path = output_dir / "stage_measurement_summary.json"
    with open(stage_path, "w") as f:
        json.dump(stage_records, f, indent=2)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Parse Spark event log zips into summary JSON")
    parser.add_argument("--input", type=Path, default=Path("data/raw/spark"),
                        help="Directory containing *.log.zip files")
    parser.add_argument("--output", type=Path, default=Path("data/processed/spark"),
                        help="Output dir for measurement_summary.json and stage_measurement_summary.json")
    args = parser.parse_args()

    if not args.input.is_dir():
        sys.exit(f"Error: input directory not found: {args.input}")

    job_records, stage_records = parse_all_logs(args.input)
    write_output(job_records, stage_records, args.output)

    print(f"Wrote {len(job_records)} jobs to {args.output / 'measurement_summary.json'}", file=sys.stderr)
    print(f"Wrote {len(stage_records)} stages to {args.output / 'stage_measurement_summary.json'}", file=sys.stderr)


if __name__ == "__main__":
    main()
