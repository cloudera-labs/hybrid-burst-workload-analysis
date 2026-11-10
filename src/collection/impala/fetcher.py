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

"""Bulk-fetch Impala queries from the Cloudera Manager API over a date range, in parallel.

The window is split into hourly chunks fetched concurrently (each paginated at 1000),
then merged and written as one combined ``impala_<from>Z_<to>Z.json`` — the same name
the disaggregation_ranking loader expects, so the output drops straight into ``data/raw/impala``.

By default the command fetches the last 7 days. Change the window with --days,
or set an exact range with --from / --to.

The command uses GET only. It reads the credentials from the environment
(CM_USER and CM_PASSWORD). It never writes the credentials to a log.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import urllib3

from ..common.cm_client import CM_BASE, create_session

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_PAGE = 1000


def _save_queries(queries: list[dict], engine: str, from_dt: str, to_dt: str, save_dir: str):
    """Save raw API response dicts to a combined JSON file in save_dir."""
    path = Path(save_dir)
    path.mkdir(parents=True, exist_ok=True)
    safe_from = from_dt.replace(":", "-").rstrip("Z").rstrip("+00:00")
    safe_to = to_dt.replace(":", "-").rstrip("Z").rstrip("+00:00")
    filename = f"{engine}_{safe_from}Z_{safe_to}Z.json"
    filepath = path / filename
    with open(filepath, "w") as f:
        json.dump(queries, f)
    print(f"Saved {len(queries)} raw queries to {filepath}", file=sys.stderr)


def _hourly_chunks(from_dt: str, to_dt: str) -> list[tuple[str, str]]:
    start = datetime.fromisoformat(from_dt.replace("Z", "+00:00"))
    end = datetime.fromisoformat(to_dt.replace("Z", "+00:00"))
    chunks = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(hours=1), end)
        chunks.append((cur.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                       nxt.strftime("%Y-%m-%dT%H:%M:%S.000Z")))
        cur = nxt
    return chunks


def _fetch_chunk(session: requests.Session, base: str, chunk_from: str,
                 chunk_to: str, retries: int = 3) -> list[dict]:
    """Fetch one hourly window, paginating until exhausted. Retries transient errors."""
    out: list[dict] = []
    offset = 0
    while True:
        for attempt in range(retries):
            try:
                resp = session.get(
                    f"{base}/impalaQueries",
                    params={"from": chunk_from, "to": chunk_to, "limit": _PAGE, "offset": offset},
                    timeout=180,
                )
                resp.raise_for_status()
                page = resp.json().get("queries", [])
                break
            except Exception:
                if attempt == retries - 1:
                    raise
                time.sleep(2 * (attempt + 1))
        out.extend(page)
        if len(page) < _PAGE:
            return out
        offset += _PAGE


def fetch_range(from_dt: str, to_dt: str, base: str, workers: int = 8) -> list[dict]:
    """Fetch all Impala queries in [from_dt, to_dt) via concurrent hourly chunks."""
    session = create_session()
    chunks = _hourly_chunks(from_dt, to_dt)
    total = len(chunks)
    results: dict[int, list[dict]] = {}
    errors: list[str] = []
    done = 0
    lock = threading.Lock()

    print(f"Fetching {total} hourly chunks ({from_dt} → {to_dt}) with {workers} workers",
          file=sys.stderr)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_chunk, session, base, cf, ct): (i, cf)
            for i, (cf, ct) in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx, cf = futures[future]
            with lock:
                done += 1
                try:
                    results[idx] = future.result()
                except Exception as e:
                    errors.append(f"{cf}: {e}")
                    print(f"  chunk {cf} FAILED: {e}", file=sys.stderr)
                if done % 12 == 0 or done == total:
                    got = sum(len(v) for v in results.values())
                    print(f"  Progress: {done}/{total} chunks ({got} queries, {len(errors)} errors)",
                          file=sys.stderr)

    if errors:
        sys.exit(f"{len(errors)} chunks failed — not writing a partial file. Rerun to retry.")

    # Concatenate in chronological order.
    queries: list[dict] = []
    for i in range(total):
        queries.extend(results.get(i, []))
    return queries


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-fetch Impala queries from the CM API over a date range, in parallel")
    parser.add_argument("--days", type=float, default=7,
                        help="Download the last N days (default: 7). --from / --to override it.")
    parser.add_argument("--from", dest="from_dt",
                        help="Start time ISO-8601, e.g. 2026-06-12T00:00:00.000Z. Overrides --days.")
    parser.add_argument("--to", dest="to_dt",
                        help="End time ISO-8601, e.g. 2026-06-19T00:00:00.000Z. Overrides --days.")
    parser.add_argument("--output", type=Path, default=Path("data/raw/impala"),
                        help="Directory to write the combined impala_*.json into")
    parser.add_argument("--cluster", default=os.environ.get("CM_CLUSTER", "cluster"),
                        help="CM cluster name (default: the CM_CLUSTER value, or 'cluster')")
    parser.add_argument("--service", default="impala", help="CM impala service name (default: impala)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Concurrent chunk-fetch threads (default: 8)")
    args = parser.parse_args()

    if not CM_BASE:
        sys.exit("Error: CM_BASE environment variable is required.\n"
                 "  export CM_BASE=\"https://<host>:<port>/api/v54\"")

    if args.from_dt and args.to_dt:
        from_dt, to_dt = args.from_dt, args.to_dt
    else:
        now = datetime.now(timezone.utc)
        to_dt = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        from_dt = (now - timedelta(days=args.days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    base = f"{CM_BASE}/clusters/{args.cluster}/services/{args.service}"
    queries = fetch_range(from_dt, to_dt, base, args.workers)

    if not queries:
        sys.exit("No queries returned for the given window.")

    _save_queries(queries, "impala", from_dt, to_dt, str(args.output))
    print(f"Done: {len(queries)} queries written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
