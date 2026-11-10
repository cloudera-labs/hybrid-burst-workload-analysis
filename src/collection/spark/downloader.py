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

"""Download Spark event log zips from the History Server, in parallel.

The command lists the completed applications for a date range in one API call.
It then downloads each event log (``/{appId}/logs``) at the same time in a thread
pool. The command skips a zip that is already present, so you can run it again to
get the files that failed.

Set the History Server address with the SPARK_HISTORY_URL environment variable,
or pass --history-url. Use the server root, for example ``https://<host>:<port>``.

By default the command downloads the last 7 days. Change the window with --days,
or set an exact range with --min-date / --max-date. Use --name-contains to keep
only one run: inside a campaign the retention window holds every earlier run, and
a directory that mixes runs is one the feature pipeline will stamp with a single
set of network constants.

Usage:
    export SPARK_HISTORY_URL="https://<host>:<port>"
    python -m src.collection.spark.downloader --days 7 --output data/raw/spark --workers 10

TLS verification is off, because the server often uses a self-signed certificate.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _resolve_base_url(cli_url: str | None) -> str:
    """Return the applications API URL from the argument or the environment.

    The function reads --history-url first, then SPARK_HISTORY_URL. It adds the
    ``/api/v1/applications`` path. It stops the program if no value is set.
    """
    root = (cli_url or os.environ.get("SPARK_HISTORY_URL", "")).rstrip("/")
    if not root:
        sys.exit("Error: set SPARK_HISTORY_URL or pass --history-url.\n"
                 "  export SPARK_HISTORY_URL=\"https://<host>:<port>\"")
    return f"{root}/api/v1/applications"


def _session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    return s


def list_app_ids(
    session: requests.Session,
    base_url: str,
    min_date: str | None,
    max_date: str | None,
    status: str = "completed",
    limit: int | None = None,
    name_contains: str | None = None,
) -> list[str]:
    """Return application IDs in the date window via a single listing call.

    ``min_date`` / ``max_date`` filter on application END time (ISO-8601 GMT, e.g.
    ``2026-06-17T00:00:00.000GMT``). The History Server returns the whole window in
    one response, so this is one API call regardless of count.

    ``name_contains`` additionally keeps only applications whose name contains that
    substring. A date window alone cannot separate one run from another: a campaign
    that runs thirteen cells back to back inside the History Server's seven-day
    retention gets every earlier cell's log in every later cell's directory, and the
    feature pipeline stamps one cell's network constants onto all of them. The
    filter is applied here, on the listing, so nothing unrelated is even downloaded.
    """
    params: dict[str, str | int] = {"status": status}
    if min_date:
        params["minEndDate"] = min_date
    if max_date:
        params["maxEndDate"] = max_date
    if limit:
        params["limit"] = limit

    resp = session.get(base_url, params=params, timeout=60)
    resp.raise_for_status()
    apps = [a for a in resp.json() if a.get("id")]
    if name_contains:
        kept = [a for a in apps if name_contains in (a.get("name") or "")]
        print(f"  name filter {name_contains!r}: kept {len(kept)} of {len(apps)} "
              f"application(s) in the window", file=sys.stderr)
        apps = kept
    return [a["id"] for a in apps]


def _download_one(
    session: requests.Session, base_url: str, app_id: str, output_dir: Path
) -> tuple[str, str]:
    """Download one app's log zip. Returns (app_id, status) where status is
    'ok', 'skip', or 'error: ...'. Partial writes go to a .part file first so an
    interrupted download never leaves a truncated zip that a rerun would skip.
    """
    dest = output_dir / f"{app_id}.log.zip"
    if dest.exists() and dest.stat().st_size > 0:
        return app_id, "skip"

    tmp = dest.with_suffix(".zip.part")
    try:
        with session.get(f"{base_url}/{app_id}/logs", stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
        tmp.rename(dest)
        return app_id, "ok"
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return app_id, f"error: {e}"


def download_logs(
    app_ids: list[str], base_url: str, output_dir: Path, workers: int = 10
) -> tuple[int, int, list[str]]:
    """Download all app logs concurrently. Returns (downloaded, skipped, errors)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    session = _session()

    total = len(app_ids)
    downloaded = skipped = 0
    errors: list[str] = []
    done = 0
    lock = threading.Lock()

    print(f"Downloading {total} event logs to {output_dir} with {workers} workers",
          file=sys.stderr)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_download_one, session, base_url, app_id, output_dir): app_id
            for app_id in app_ids
        }
        for future in as_completed(futures):
            app_id, status = future.result()
            with lock:
                done += 1
                if status == "ok":
                    downloaded += 1
                elif status == "skip":
                    skipped += 1
                else:
                    errors.append(f"{app_id}: {status}")
                    print(f"  {app_id} {status}", file=sys.stderr)
                if done % 25 == 0 or done == total:
                    print(f"  Progress: {done}/{total} "
                          f"({downloaded} new, {skipped} skipped, {len(errors)} errors)",
                          file=sys.stderr)

    return downloaded, skipped, errors


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Spark event log zips from the History Server, in parallel")
    parser.add_argument("--history-url",
                        help="History Server root, e.g. https://<host>:<port>. "
                        "Defaults to the SPARK_HISTORY_URL environment variable.")
    parser.add_argument("--days", type=float, default=7,
                        help="Download the last N days (default: 7). "
                        "--min-date / --max-date override it.")
    parser.add_argument("--min-date", help="Filter apps that ended after this ISO-8601 GMT time, "
                        "e.g. 2026-06-17T00:00:00.000GMT. Overrides --days.")
    parser.add_argument("--max-date", help="Filter apps that ended before this ISO-8601 GMT time. "
                        "Overrides --days.")
    parser.add_argument("--status", default="completed",
                        choices=["completed", "running", "failed"],
                        help="Application status filter (default: completed)")
    parser.add_argument("--limit", type=int, help="Cap the number of apps listed")
    parser.add_argument("--name-contains",
                        help="Keep only applications whose name contains this substring. "
                        "Use it to separate one run from another inside the retention "
                        "window; a date range alone cannot.")
    parser.add_argument("--output", type=Path, default=Path("data/raw/spark"),
                        help="Directory to write *.log.zip files into")
    parser.add_argument("--workers", type=int, default=10,
                        help="Concurrent download threads (default: 10)")
    args = parser.parse_args()

    base_url = _resolve_base_url(args.history_url)

    # Default to the last N days when no explicit window is given. The History Server
    # filters on application END time, so a fixed max is not needed for "the last week".
    min_date, max_date = args.min_date, args.max_date
    if not min_date and not max_date:
        min_date = (datetime.now(timezone.utc) - timedelta(days=args.days)).strftime(
            "%Y-%m-%dT%H:%M:%S.000GMT")

    session = _session()
    print("Listing applications...", file=sys.stderr)
    try:
        app_ids = list_app_ids(session, base_url, min_date, max_date, args.status,
                               args.limit, args.name_contains)
    except requests.RequestException as e:
        sys.exit(f"Error listing applications: {e}")

    if not app_ids:
        sys.exit("No applications matched the given filters.")
    print(f"Found {len(app_ids)} applications", file=sys.stderr)

    downloaded, skipped, errors = download_logs(app_ids, base_url, args.output, args.workers)

    print(f"\nDone: {downloaded} downloaded, {skipped} already present, {len(errors)} errors",
          file=sys.stderr)
    if errors:
        sys.exit(f"{len(errors)} downloads failed (rerun to retry — existing files are skipped)")


if __name__ == "__main__":
    main()
