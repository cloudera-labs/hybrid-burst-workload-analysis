import json
import os
import stat
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "hdfs_table_sizes.py"

# A stand-in for the hdfs and beeline commands, so the script runs as it would on a
# gateway node. It answers -count for two known paths and fails the rest the way
# HDFS does; it records every invocation, so the test can check nothing else ran.
_FAKE_HDFS = """#!/bin/sh
echo "hdfs $*" >> "$CALLS"
shift 2
for p in "$@"; do
  case "$p" in
    hdfs://ns/wh/s.db/orders) echo "           4          120         5000000 hdfs://ns/wh/s.db/orders" ;;
    hdfs://ns/wh/s.db/found) echo "           1            2             700 hdfs://ns/wh/s.db/found" ;;
    *) echo "count: \\`$p': No such file or directory" >&2 ;;
  esac
done
exit 1
"""
# Answers a batch: prints each marker, and a Location only for s.found - the
# other table in the batch is gone, as a dropped staging table would be.
_FAKE_BEELINE = """#!/bin/sh
echo "beeline $*" >> "$CALLS"
for last; do :; done
echo "$last" | tr ';' '\\n' | while read -r stmt; do
  case "$stmt" in
    "SELECT '__table__ "*) t=${stmt#SELECT \\'__table__ }; t=${t%\\'}; echo "_c0"; echo "__table__ $t" ;;
    "DESCRIBE FORMATTED s.found") printf 'col_name\\tdata_type\\tcomment\\n'; printf 'Location:\\thdfs://ns/wh/s.db/found\\tNULL\\n' ;;
  esac
done
"""


def _bin(tmp_path):
    d = tmp_path / "bin"
    d.mkdir()
    for name, body in (("hdfs", _FAKE_HDFS), ("beeline", _FAKE_BEELINE)):
        p = d / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IEXEC)
    return d


def _run(tmp_path, inventory, *extra):
    inv = tmp_path / "inv.json"
    inv.write_text(json.dumps(inventory))
    calls = tmp_path / "calls.txt"
    env = {**os.environ, "PATH": f"{_bin(tmp_path)}:{os.environ['PATH']}", "CALLS": str(calls)}
    out = tmp_path / "sizes.json"
    p = subprocess.run([sys.executable, str(SCRIPT), str(inv), "-o", str(out), *extra],
                       env=env, capture_output=True, text=True)
    assert p.returncode == 0, p.stderr
    called = calls.read_text().splitlines() if calls.exists() else []
    return (json.loads(out.read_text()) if out.exists() else None), called, p


_INVENTORY = [
    {"table": "s.orders", "storage": "hdfs", "location_root": "hdfs://ns/wh/s.db/orders", "location_truncated": False},
    {"table": "s.gone", "storage": "hdfs", "location_root": "hdfs://ns/wh/s.db/gone", "location_truncated": False},
    {"table": "s.found", "storage": "hdfs", "location_root": None, "location_truncated": None},
    {"table": "s.cut", "storage": "hdfs", "location_root": "hdfs://ns/wh/s.db/cu", "location_truncated": True},
    {"table": "ops.accounts", "storage": "kudu", "location_root": None, "location_truncated": None},
    {"table": "s.dropped", "storage": "hdfs", "location_root": None, "location_truncated": None},
]


def test_sizes_by_status(tmp_path):
    doc, called, _ = _run(tmp_path, _INVENTORY)
    by = {t["table"]: t for t in doc["tables"]}
    assert (by["s.orders"]["status"], by["s.orders"]["bytes"], by["s.orders"]["files"]) == ("ok", 5000000, 120)
    assert by["s.gone"]["status"] == "missing"
    assert by["s.found"]["status"] == "no-location"   # no beeline URL given
    assert by["s.cut"]["status"] == "no-location"     # a truncated root is never measured
    assert by["ops.accounts"]["status"] == "not-hdfs"
    assert all(c.startswith("hdfs dfs -count ") for c in called)
    assert doc["collected_at"].endswith("Z")


def test_beeline_resolves_a_missing_location(tmp_path):
    doc, called, _ = _run(tmp_path, _INVENTORY, "--beeline-url", "jdbc:hive2://h:10000/")
    by = {t["table"]: t for t in doc["tables"]}
    assert (by["s.found"]["status"], by["s.found"]["bytes"]) == ("ok", 700)
    assert by["s.dropped"]["status"] == "missing" and "dropped" in by["s.dropped"]["error"]
    beeline = [c for c in called if c.startswith("beeline")]
    assert len(beeline) == 1  # one session for the whole batch
    assert "DESCRIBE FORMATTED s.found" in beeline[0] and "DESCRIBE FORMATTED s.cut" in beeline[0]


def test_dry_run_runs_nothing(tmp_path):
    doc, called, p = _run(tmp_path, _INVENTORY, "--dry-run")
    assert doc is None and called == []
    assert "hdfs dfs -count" in p.stdout
