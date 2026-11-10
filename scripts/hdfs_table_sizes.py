#!/usr/bin/env python3
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

"""Measure the total size of a list of tables on HDFS. Read-only.

This script is meant to be handed to a cluster's administrators, so it is
self-contained: Python 3.8+, the standard library, and the ``hdfs`` command
(and optionally ``beeline``) that every gateway node has. It changes nothing.
The only commands it runs are:

    hdfs dfs -count <path> [<path> ...]          directory size, file count
    beeline -u <url> -e "DESCRIBE FORMATTED t"   only with --beeline-url, to
                                                 find a table's location; 100
                                                 tables per beeline session

Run ``kinit`` first; the script handles no credentials. ``--dry-run`` prints
every command without running any, for review.

Input: the ``table_inventory.json`` that ``table_access`` writes - a list of
``{table, storage, location_root, location_truncated}`` - or the same list under
a ``"tables"`` key. Output: one JSON document,

    {"collected_at": ..., "host": ..., "tables": [
        {"table", "location", "bytes", "files", "dirs", "status", "error"}]}

``status`` is ``ok``, ``missing`` (the path does not exist), ``no-location``
(none was given and none could be found), ``not-hdfs`` (Kudu, JDBC: no HDFS
directory to measure) or ``error``. ``bytes`` is the logical size, before
replication - what ``hdfs dfs -count`` reports as CONTENT_SIZE.

Usage:
    python3 hdfs_table_sizes.py table_inventory.json -o table_sizes.json
    python3 hdfs_table_sizes.py table_inventory.json --beeline-url 'jdbc:hive2://...' -o table_sizes.json
    python3 hdfs_table_sizes.py table_inventory.json --dry-run
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import socket
import subprocess
import sys

VERSION = 1
BATCH = 50
_COUNT_LINE = re.compile(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S.*)$")
_LOCATION_LINE = re.compile(r"^\s*Location:?\s*\|?\s*(\S+?)\s*\|?\s*$", re.IGNORECASE)


def load(path: str) -> list:
    with open(path) as f:
        data = json.load(f)
    tables = data["tables"] if isinstance(data, dict) else data
    out = []
    for t in tables:
        out.append({
            "table": t["table"],
            "storage": t.get("storage") or "hdfs",
            "location": None if t.get("location_truncated") else t.get("location_root"),
        })
    return out


def measurable(path: str) -> bool:
    return path.startswith("hdfs://") or path.startswith("/")


def run(cmd: list, dry: bool, timeout: int) -> tuple:
    if dry:
        print(" ".join(cmd if len(cmd) < 8 else cmd[:7] + [f"... ({len(cmd) - 7} more paths)"]))
        return 0, "", ""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout} s"
    except FileNotFoundError:
        sys.exit(f"Error: {cmd[0]} not found on PATH")
    return p.returncode, p.stdout, p.stderr


LOOKUP_BATCH = 100
_NOT_FOUND = "no Location in DESCRIBE FORMATTED: dropped, or not a Hive table"
_MARK = "__table__ "


def resolve_locations(tables: list, url: str, dry: bool, timeout: int) -> dict:
    """``{table: (location, error)}`` from ``DESCRIBE FORMATTED``, LOOKUP_BATCH
    tables per beeline session: one session per table would spend most of its
    time connecting. Each table is preceded by a ``SELECT`` of a marker so the
    output can be split back per table, and ``--force`` keeps going past a table
    that no longer exists."""
    out_map = {}
    plain = [t for t in tables if re.fullmatch(r"[\w$-]+(\.[\w$-]+)?", t)]
    for t in tables:
        if t not in plain:
            out_map[t] = (None, "not a plain db.table name")
    for i in range(0, len(plain), LOOKUP_BATCH):
        batch = plain[i:i + LOOKUP_BATCH]
        script = " ".join(f"SELECT '{_MARK}{t}'; DESCRIBE FORMATTED {t};" for t in batch)
        cmd = ["beeline", "-u", url, "--silent=true", "--force=true", "--outputformat=tsv2", "-e", script]
        code, out, err = run(cmd, dry, timeout)
        if dry:
            continue
        current = None
        for line in out.splitlines():
            if line.startswith(_MARK):
                current = line[len(_MARK):].strip()
                continue
            if current is None or current in out_map:
                continue
            cells = [c.strip() for c in line.split("\t")]
            if cells and cells[0].rstrip(":").lower() == "location" and len(cells) > 1 and cells[1]:
                out_map[current] = (cells[1], None)
                continue
            m = _LOCATION_LINE.match(line)
            if m:
                out_map[current] = (m.group(1), None)
        for t in batch:
            if t not in out_map:
                why = "timed out" if code == 124 else _NOT_FOUND
                out_map[t] = (None, why)
    return out_map


def count(paths: list, dry: bool, timeout: int) -> tuple:
    """Size of each path. One ``hdfs dfs -count`` per batch: it prints a line per
    path that exists and an error per path that does not, and keeps going."""
    sizes, errors = {}, {}
    for i in range(0, len(paths), BATCH):
        batch = paths[i:i + BATCH]
        code, out, err = run(["hdfs", "dfs", "-count"] + batch, dry, timeout)
        for line in out.splitlines():
            m = _COUNT_LINE.match(line)
            if m:
                dirs, files, size, path = m.groups()
                sizes[path.strip()] = (int(size), int(files), int(dirs))
        for line in err.splitlines():
            for p in batch:
                if p in line:
                    errors[p] = line.strip()[:300]
        if code == 124:
            for p in batch:
                errors.setdefault(p, err)
    return sizes, errors


def lookup(sizes: dict, path: str):
    if path in sizes:
        return sizes[path]
    # ``hdfs dfs -count`` prints the path as given or fully qualified, depending on
    # the version; compare without the scheme and authority as a fallback.
    bare = re.sub(r"^hdfs://[^/]*", "", path)
    for p, v in sizes.items():
        if re.sub(r"^hdfs://[^/]*", "", p) == bare:
            return v
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Measure the HDFS size of each table. Read-only.")
    ap.add_argument("inventory", help="table_inventory.json, or {\"tables\": [...]}")
    ap.add_argument("-o", "--output", help="where to write the result (default: stdout)")
    ap.add_argument("--beeline-url", help="HiveServer2 JDBC URL, to look up tables with no known location")
    ap.add_argument("--dry-run", action="store_true", help="print the commands, run nothing")
    ap.add_argument("--timeout", type=int, default=300, help="seconds per command (default 300)")
    args = ap.parse_args()

    tables = load(args.inventory)
    lookups = {}
    if args.beeline_url:
        wanted = [t["table"] for t in tables
                  if t["storage"] == "hdfs" and not t["location"] and not t["table"].startswith("path:")]
        lookups = resolve_locations(wanted, args.beeline_url, args.dry_run, args.timeout)
    result = []
    for t in tables:
        r = {"table": t["table"], "location": t["location"], "bytes": None, "files": None,
             "dirs": None, "status": None, "error": None}
        if t["storage"] != "hdfs":
            r["status"] = "not-hdfs"
        elif t["table"] in lookups:
            r["location"], r["error"] = lookups[t["table"]]
        if r["status"] is None and not r["location"]:
            r["status"] = "missing" if r["error"] == _NOT_FOUND else "no-location"
        elif r["status"] is None and not measurable(r["location"]):
            r["status"], r["error"] = "not-hdfs", "location is not on HDFS"
        result.append(r)

    todo = sorted({r["location"] for r in result if r["status"] is None})
    sizes, errors = count(todo, args.dry_run, args.timeout)
    if args.dry_run:
        print(f"# {len(todo)} paths to measure, {sum(1 for r in result if r['status'])} tables skipped",
              file=sys.stderr)
        return
    for r in result:
        if r["status"] is not None:
            continue
        v = lookup(sizes, r["location"])
        if v is not None:
            r["bytes"], r["files"], r["dirs"] = v
            r["status"] = "ok"
        else:
            msg = errors.get(r["location"], "")
            r["status"] = "missing" if "No such file" in msg else "error"
            r["error"] = msg or "no output for this path"

    doc = {
        "script_version": VERSION,
        "collected_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "host": socket.getfqdn(),
        "tables": result,
    }
    text = json.dumps(doc, indent=1)
    if args.output:
        with open(args.output, "w") as f:
            f.write(text + "\n")
    else:
        print(text)
    by = {}
    for r in result:
        by[r["status"]] = by.get(r["status"], 0) + 1
    print("tables by status: " + ", ".join(f"{k} {v}" for k, v in sorted(by.items())), file=sys.stderr)


if __name__ == "__main__":
    main()
