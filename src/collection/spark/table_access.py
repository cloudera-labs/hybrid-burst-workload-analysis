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

"""Which tables a Spark application reads and writes, and how much of each.

The question this answers is a placement one: if a set of applications ran
somewhere else, which tables would have to be there too, and how big are they?
`event_parser` reads table names only to fingerprint a recurring job, and must
keep doing exactly that - widening its regex would move every ``match_id``. This
module is a separate pass with its own patterns, so it can be as wide as the plans
require without touching the grouping.

What the event log gives, per SQL execution:

- **Scans** in ``sparkPlanInfo``. A file scan names its table (``Scan parquet
  db.tbl``), carries ``metadata.Location`` and ``PartitionFilters``, and has the
  driver metric ``size of files read`` - the size of the files it touched after
  pruning. Hive (``HiveTableRelation``), Iceberg (``BatchScan``), Kudu and JDBC
  scans name their table but carry no size; their bytes come from the input of
  the stages that ran them, split by output rows, and are labelled so.
- **Writes**, as command nodes: ``InsertIntoHiveTable``, the CTAS commands,
  ``InsertIntoHadoopFsRelationCommand`` (whose ``CatalogTable`` also gives the
  table's location), Iceberg ``AppendData``/``ReplaceData``, and Kudu through
  ``SaveIntoDataSourceCommand``. V1 writes carry ``written output``; the others
  get the output bytes of their execution's stages.

Kudu and Kafka move bytes Spark does not count: their scans and writes leave
``bytesRead``/``bytesWritten`` at 0. The Kudu connector does count rows, in its
own stage accumulators (``kudu.rows_read``, ``kudu.num_inserts`` and the like,
the write counters as ``{table=rows}`` maps). Those rows are kept as counted.
Where a physical ``Scan Kudu`` shows the columns it projects, ``bytes_guess``
turns them into bytes at ``GUESS_BYTES_PER_COLUMN`` per column - a guess, flagged
``guessed`` with its ``guess_basis``, never written into ``bytes``. A view's plan
does not show which columns Kudu returns (its relation lists all of them, and the
same view may be scanned once for one column and once for all), so there the
bytes stay null: on the first prod week, guessing them from the relation's width
came to 116 TB against 53 TB measured. A Kafka write (Envelope's
``KafkaOutput``) records neither topic nor rows; it is listed as a target with
both null. Many of these apps run their Kudu reads through views and
``Dataset.javaRDD``, outside any SQL execution, so a table can be known only
from a view's plan text (``LogicalRelation ... Kudu db.t, [cols]``).

What it never gives is a table's TOTAL size: that is a property of the storage,
not of a query. ``full_scan_bytes_max`` - the largest unpruned scan of a table -
is a lower bound and nothing more. ``scripts/hdfs_table_sizes.py`` measures the
real figure from the inventory this module writes.

Usage:
    python -m src.collection.spark.table_access --input data/raw/spark --output data/processed/spark
"""

from __future__ import annotations

import json
import re
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

# ── Patterns ───────────────────────────────────────────────────────────────────

_FILE_FORMATS = "parquet|orc|json|csv|avro|text|delta"
_FILE_SCAN_RE = re.compile(rf"^Scan ({_FILE_FORMATS})\s*(\S*)\s*$", re.IGNORECASE)
_HIVE_SCAN_RE = re.compile(r"^Scan hive\s+(\S+)", re.IGNORECASE)
_BATCH_SCAN_RE = re.compile(r"^BatchScan\s+(\S+)")
_KUDU_SCAN_RE = re.compile(r"^Scan Kudu\s+(\S+)")
_JDBC_SCAN_RE = re.compile(r"^Scan JDBCRelation\((.+?)\)")

# A quoted or plain dotted identifier at the start of the arguments.
_IDENT = r"((?:`[^`]+`|[\w$-]+)(?:\.(?:`[^`]+`|[\w$-]+))*)"
_INSERT_HIVE_RE = re.compile(rf"InsertIntoHiveTable\s+{_IDENT}")
_CTAS_DS_RE = re.compile(rf"CreateDataSourceTableAsSelectCommand\s+{_IDENT}")
_CTAS_HIVE_RE = re.compile(r"CreateHiveTableAsSelectCommand\s+\[Database:\s*([^,\]]+),\s*TableName:\s*([^,\]]+)")
_INSERT_FS_RE = re.compile(r"InsertIntoHadoopFsRelationCommand\s+([^,\s]+)")
_CATALOG_TABLE_RE = re.compile(r"CatalogTable\(\s*(?:Catalog:\s*\S+\s+)?Database:\s*(\S+)\s+Table:\s*(\S+)")
_CATALOG_LOCATION_RE = re.compile(r"\nLocation:\s*(\S+)")
_ICEBERG_WRITE_RE = re.compile(r"IcebergWrite\(table=([^,)\s]+)")
_KUDU_WRITE_RE = re.compile(r"kudu\.table=([^,\]\s]+)")
_PLAN_SECTION_RE = re.compile(r"^\((\d+)\) (\w+)", re.MULTILINE)

# Kudu columns: a physical scan lists the ones it projects, ``Scan Kudu
# impala::db.t [a#1,b#2]``; a view's plan text lists the relation's, ``Kudu
# impala::db.t, [a#1, b#2, ... 40 more fields]``.
_KUDU_SCAN_COLS_RE = re.compile(r"^Scan Kudu\s+\S+\s+\[([^\]]*)\]?")
_KUDU_RELATION_RE = re.compile(r"Kudu (\S+), \[([^\]]*)\]")
_MORE_FIELDS_RE = re.compile(r"\.\.\.\s*(\d+) more fields")
_COUNTER_MAP_RE = re.compile(r"([^{},=\s][^{},=]*)=(\d+)")
_KAFKA_SINK_RE = re.compile(r"KafkaOutput|KafkaWriter|KafkaSink|KafkaDataWriter|ProducerRecord")
_KAFKA_SOURCE_RE = re.compile(r"KafkaRDD|KafkaSourceRDD|KafkaMicroBatch")

_KUDU_ROWS_READ = "kudu.rows_read"
_KUDU_WRITES = ("kudu.num_inserts", "kudu.num_upserts", "kudu.num_updates", "kudu.num_deletes")

# A guessed row is this many bytes per projected column: between an 8-byte number
# and a short string. One constant, written
# into every guess_basis, so a different assumption is one edit and a re-run.
GUESS_BYTES_PER_COLUMN = 16

_LOCATION_RE = re.compile(r"^\w+\((\d+) paths?\)\[(.*)\]$", re.DOTALL)
_PARTITION_SEGMENT_RE = re.compile(r"^[^=/]+=[^/]*$")

_SIZE = "size of files read"
_FILES = "number of files read"
_PARTITIONS = "number of partitions read"
_ROWS = "number of output rows"
_WRITTEN_BYTES = "written output"
_WRITTEN_FILES = "number of written files"

_INPUT_BYTES = "internal.metrics.input.bytesRead"
_OUTPUT_BYTES = "internal.metrics.output.bytesWritten"

# Event types this pass needs. Everything else - above all the per-task events,
# which are most of a log - is skipped before it is decoded.
_WANTED = (
    b"SparkListenerApplicationStart",
    b"SparkListenerJobStart",
    b"SparkListenerStageCompleted",
    b"SQLExecutionStart",
    b"SQLAdaptiveExecutionUpdate",
    b"SparkListenerDriverAccumUpdates",
)


def normalise_table(ident: str) -> str:
    """``spark_catalog.db.tbl``, `` `db`.`tbl` `` and ``impala::db.tbl`` all name
    ``db.tbl``: one table, whichever engine or catalog wrote the plan."""
    t = ident.strip().strip(",").replace("`", "")
    if t.startswith("impala::"):
        t = t[len("impala::"):]
    if t.lower().startswith("spark_catalog."):
        t = t[len("spark_catalog."):]
    return t.lower()


def location_root(location: str) -> tuple[Optional[str], bool]:
    """The table directory behind a scan's ``Location``, and whether Spark cut it.

    A scan of a partitioned table lists partition directories
    (``.../tbl/dt=2026-10-08``); the table is what is left after the trailing
    ``key=value`` segments go. Spark truncates long metadata strings with ``...``,
    and a listing that shows fewer paths than it counts is truncated too.
    """
    m = _LOCATION_RE.match(location.strip())
    if not m:
        return None, False
    count = int(m.group(1))
    body = m.group(2).strip()
    truncated = body.endswith("...")
    paths = [p.strip() for p in body.split(",") if p.strip() and p.strip() != "..."]
    if truncated and paths:
        paths[-1] = paths[-1].rstrip(".")
    if len(paths) < count:
        truncated = True
    roots = [r for r in (strip_partitions(p) for p in paths) if r]
    if not roots:
        return None, truncated
    return common_root(roots), truncated


def strip_partitions(path: str) -> str:
    segs = path.rstrip("/").split("/")
    while len(segs) > 3 and _PARTITION_SEGMENT_RE.match(segs[-1]):
        segs.pop()
    return "/".join(segs)


def common_root(paths: list[str]) -> str:
    split = [p.split("/") for p in paths]
    out = []
    for parts in zip(*split):
        if len(set(parts)) != 1:
            break
        out.append(parts[0])
    return "/".join(out)


# ── Per-app state ──────────────────────────────────────────────────────────────


@dataclass
class _Scan:
    table: Optional[str]
    storage: str
    format: str
    root: Optional[str] = None
    truncated: bool = False
    full_scan: Optional[bool] = None
    acc: dict[str, int] = field(default_factory=dict)  # metric name -> accumulator id
    columns: Optional[int] = None  # Kudu: the columns the scan projects


@dataclass
class _Write:
    table: Optional[str]
    storage: str
    format: str
    execution: int
    root: Optional[str] = None
    acc: dict[str, int] = field(default_factory=dict)


class TableAccessState:
    def __init__(self) -> None:
        self.app_id = ""
        self.scans: dict[str, _Scan] = {}
        self.writes: dict[str, _Write] = {}
        self.driver_acc: dict[int, int] = {}
        self.stage_acc: dict[int, dict[int, int]] = defaultdict(dict)  # acc id -> stage -> value
        self.stage_input: dict[int, int] = {}
        self.stage_output: dict[int, int] = {}
        self.stage_execution: dict[int, int] = {}
        self.unresolved_writes = 0
        # Kudu and Kafka, which Spark's byte counters miss.
        self.kudu_relation_columns: dict[str, int] = {}          # table -> columns in a view's plan
        self.kudu_rows_read: dict[int, tuple[int, int]] = {}     # acc id -> (stage, value)
        self.kudu_rows_written: dict[tuple[int, str], int] = {}  # (acc id, table) -> rows
        self.kafka_sink_stages: set[int] = set()
        self.kafka_source_stages: set[int] = set()


def _metric_ids(node: dict) -> dict[str, int]:
    out = {}
    for m in node.get("metrics", []) or []:
        try:
            out[str(m.get("name", "")).strip().lower()] = int(m["accumulatorId"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _node_key(acc: dict[str, int], fallback: str) -> str:
    # The same physical node keeps its accumulators across AQE re-plans, so they
    # identify it; a node without metrics falls back to its place in the plan.
    return f"a{min(acc.values())}" if acc else fallback


def count_columns(listing: str) -> tuple[int, bool]:
    """Columns in a plan's ``[a#1, b#2, ... 3 more fields]``, and whether the list
    was cut without saying how much (a trailing ``...``): then it is a floor."""
    more = _MORE_FIELDS_RE.search(listing)
    body = _MORE_FIELDS_RE.sub("", listing)
    names = [c for c in (x.strip() for x in body.split(",")) if c and c != "..."]
    cut = not more and body.rstrip().endswith("...")
    return len(names) + (int(more.group(1)) if more else 0), cut


def relation_columns_used(listing: str, rest_of_plan: str) -> int:
    """How many of a view's relation columns its plan uses: the ones whose
    ``name#id`` appears elsewhere in the plan. A relation that is the view's whole
    output references nothing else, and a cut list whose every shown column is used
    (a ``select *``) is taken as all of them."""
    total = count_columns(listing)[0]
    ids = re.findall(r"#(\d+)", _MORE_FIELDS_RE.sub("", listing))
    used = sum(1 for i in ids if re.search(rf"#{i}\b", rest_of_plan))
    if used == 0 or (used == len(ids) and total > len(ids)):
        return total
    return used


def _classify_scan(node: dict) -> Optional[_Scan]:
    name = str(node.get("nodeName", "")).strip()
    meta = node.get("metadata") or {}
    m = _FILE_SCAN_RE.match(name)
    if m:
        scan = _Scan(table=normalise_table(m.group(2)) if m.group(2) else None,
                     storage="hdfs", format=m.group(1).lower())
        if "Location" in meta:
            scan.root, scan.truncated = location_root(str(meta["Location"]))
            if scan.root and "://" in scan.root and not scan.root.startswith("hdfs://"):
                scan.storage = scan.root.split("://", 1)[0]
        if "PartitionFilters" in meta:
            scan.full_scan = str(meta["PartitionFilters"]).strip() == "[]"
        return scan
    for rx, storage, fmt in ((_HIVE_SCAN_RE, "hdfs", "hive"), (_BATCH_SCAN_RE, "hdfs", "iceberg"),
                             (_KUDU_SCAN_RE, "kudu", "kudu")):
        m = rx.match(name)
        if m:
            scan = _Scan(table=normalise_table(m.group(1)), storage=storage, format=fmt)
            if storage == "kudu":
                cols = _KUDU_SCAN_COLS_RE.match(str(node.get("simpleString", "")))
                if cols:
                    scan.columns = count_columns(cols.group(1))[0] or None
            return scan
    m = _JDBC_SCAN_RE.match(name)
    if m:
        return _Scan(table=f"jdbc:{m.group(1).strip()}", storage="jdbc", format="jdbc")
    return None


def _classify_write(node: dict, execution: int) -> Optional[_Write]:
    s = str(node.get("simpleString", ""))
    name = str(node.get("nodeName", ""))
    if "InsertIntoHiveTable" in name:
        m = _INSERT_HIVE_RE.search(s)
        return _Write(normalise_table(m.group(1)) if m else None, "hdfs", "hive", execution)
    if "CreateHiveTableAsSelectCommand" in name:
        m = _CTAS_HIVE_RE.search(s)
        t = normalise_table(f"{m.group(1)}.{m.group(2)}") if m else None
        return _Write(t, "hdfs", "hive", execution)
    if "CreateDataSourceTableAsSelectCommand" in name:
        m = _CTAS_DS_RE.search(s)
        return _Write(normalise_table(m.group(1)) if m else None, "hdfs", "file", execution)
    if "InsertIntoHadoopFsRelationCommand" in name:
        w = _Write(None, "hdfs", "file", execution)
        m = _CATALOG_TABLE_RE.search(s)
        if m:
            w.table = normalise_table(f"{m.group(1)}.{m.group(2)}")
        loc = _CATALOG_LOCATION_RE.search(s)
        path = _INSERT_FS_RE.search(s)
        if loc:
            w.root = loc.group(1)
        elif path and "://" in path.group(1):
            w.root = strip_partitions(path.group(1))
        return w
    if name in ("AppendData", "OverwriteByExpression", "OverwritePartitionsDynamic"):
        m = _ICEBERG_WRITE_RE.search(s)
        return _Write(normalise_table(m.group(1)) if m else None, "hdfs", "iceberg", execution)
    return None


def _walk(node: Any, state: TableAccessState, execution: int, where: str) -> None:
    if not isinstance(node, dict):
        return
    acc = _metric_ids(node)
    scan = _classify_scan(node)
    if scan is not None:
        scan.acc = acc
        key = _node_key(acc, f"x{execution}:{where}")
        prior = state.scans.get(key)
        # An adaptive re-plan repeats the node; keep the record that knows most.
        if prior is None or (prior.root is None and scan.root is not None):
            state.scans[key] = scan
    write = _classify_write(node, execution)
    if write is not None:
        write.acc = acc
        key = _node_key(acc, f"x{execution}:{where}")
        state.writes.setdefault(key, write)
    for i, child in enumerate(node.get("children", []) or []):
        _walk(child, state, execution, f"{where}.{i}")


def _writes_from_description(desc: str, state: TableAccessState, execution: int) -> None:
    """Row-level Iceberg writes and Kudu writes name their target only in the plan
    text (``Arguments: IcebergWrite(table=...)``, ``kudu.table=...``)."""
    if not desc:
        return
    known = {(w.execution, w.table) for w in state.writes.values()}
    sections = list(_PLAN_SECTION_RE.finditer(desc))
    for i, sec in enumerate(sections):
        op = sec.group(2)
        if op not in ("ReplaceData", "WriteDelta", "SaveIntoDataSourceCommand"):
            continue
        end = sections[i + 1].start() if i + 1 < len(sections) else len(desc)
        body = desc[sec.start():end]
        table, storage, fmt = None, "hdfs", "iceberg"
        if op == "SaveIntoDataSourceCommand":
            m = _KUDU_WRITE_RE.search(body)
            if not m:
                state.unresolved_writes += 1
                continue
            table, storage, fmt = normalise_table(m.group(1)), "kudu", "kudu"
        else:
            m = _ICEBERG_WRITE_RE.search(body)
            if m:
                table = normalise_table(m.group(1))
        if table is None or (execution, table) not in known:
            key = f"x{execution}:{op}:{sec.group(1)}"
            state.writes.setdefault(key, _Write(table, storage, fmt, execution))


def _resolve_row_level_targets(state: TableAccessState, execution_scans: dict[int, set[str]]) -> None:
    """A ``WriteDelta`` (an Iceberg MERGE or UPDATE) never names its target, but it
    reads the target in the same execution. With exactly one Iceberg table scanned
    there, that table is the target; otherwise it stays unresolved."""
    for w in state.writes.values():
        if w.table is None and w.format == "iceberg":
            tables = {state.scans[k].table for k in execution_scans.get(w.execution, set())
                      if state.scans[k].format == "iceberg"}
            if len(tables) == 1:
                w.table = tables.pop()


def process_event(event: dict, state: TableAccessState, execution_scans: dict[int, set[str]]) -> None:
    kind = event.get("Event", "")
    if kind == "SparkListenerApplicationStart":
        state.app_id = event.get("App ID", "")
    elif kind.endswith("SQLExecutionStart") or kind.endswith("SQLAdaptiveExecutionUpdate"):
        try:
            execution = int(event.get("executionId", -1))
        except (TypeError, ValueError):
            execution = -1
        _walk(event.get("sparkPlanInfo"), state, execution, "0")
        _collect_scan_keys(event.get("sparkPlanInfo"), state, execution, execution_scans)
        if kind.endswith("SQLExecutionStart"):
            desc = event.get("physicalPlanDescription", "")
            _writes_from_description(desc, state, execution)
            for m in _KUDU_RELATION_RE.finditer(desc or ""):
                t = normalise_table(m.group(1))
                n = relation_columns_used(m.group(2), desc[:m.start()] + desc[m.end():])
                state.kudu_relation_columns[t] = max(state.kudu_relation_columns.get(t, 0), n)
    elif kind.endswith("SparkListenerDriverAccumUpdates"):
        for update in event.get("accumUpdates", []) or []:
            try:
                state.driver_acc[int(update[0])] = int(update[1])
            except (TypeError, ValueError, IndexError):
                continue
    elif kind == "SparkListenerJobStart":
        props = event.get("Properties") or {}
        try:
            execution = int(props.get("spark.sql.execution.id"))
        except (TypeError, ValueError):
            return
        for sid in event.get("Stage IDs", []) or []:
            state.stage_execution[int(sid)] = execution
    elif kind == "SparkListenerStageCompleted":
        info = event.get("Stage Info") or {}
        try:
            sid = int(info["Stage ID"])
        except (KeyError, TypeError, ValueError):
            return
        details = f"{info.get('Stage Name', '')}\n{info.get('Details', '')}"
        if _KAFKA_SINK_RE.search(details):
            state.kafka_sink_stages.add(sid)
        if _KAFKA_SOURCE_RE.search(details):
            state.kafka_source_stages.add(sid)
        for a in info.get("Accumulables", []) or []:
            name = a.get("Name", "")
            if name in _KUDU_WRITES:
                # ``{impala::db.t=12}``: a value is the accumulator's total so far, so keep the largest.
                try:
                    aid = int(a["ID"])
                except (KeyError, TypeError, ValueError):
                    continue
                for table, rows in _COUNTER_MAP_RE.findall(str(a.get("Value", ""))):
                    key = (aid, normalise_table(table))
                    state.kudu_rows_written[key] = max(state.kudu_rows_written.get(key, 0), int(rows))
                continue
            try:
                aid, value = int(a["ID"]), int(float(a.get("Value", 0)))
            except (KeyError, TypeError, ValueError):
                continue
            if name == _KUDU_ROWS_READ:
                prior = state.kudu_rows_read.get(aid)
                if prior is None or value > prior[1]:
                    state.kudu_rows_read[aid] = (sid, value)
                continue
            if name == _INPUT_BYTES:
                state.stage_input[sid] = value
            elif name == _OUTPUT_BYTES:
                state.stage_output[sid] = value
            else:
                state.stage_acc[aid][sid] = value


def _collect_scan_keys(node: Any, state: TableAccessState, execution: int,
                       execution_scans: dict[int, set[str]], where: str = "0") -> None:
    if not isinstance(node, dict):
        return
    if _classify_scan(node) is not None:
        key = _node_key(_metric_ids(node), f"x{execution}:{where}")
        if key in state.scans:
            execution_scans.setdefault(execution, set()).add(key)
    for i, child in enumerate(node.get("children", []) or []):
        _collect_scan_keys(child, state, execution, execution_scans, f"{where}.{i}")


# ── Results ────────────────────────────────────────────────────────────────────


@dataclass
class TableAccessRow:
    """One (application, table, direction). ``table`` is null for a scan of a bare
    path; ``location_root`` then identifies it, and the inventory matches it to a
    table by location."""
    app_id: str
    table: Optional[str]
    direction: str           # read | write
    storage: str             # hdfs | kudu | jdbc | <scheme>
    format: str
    bytes: Optional[int]     # decimal bytes
    bytes_source: Optional[str]  # files | stage | metric | mixed
    files: Optional[int]
    partitions: Optional[int]
    operations: int          # scan or write nodes folded into this row
    full_scan_bytes_max: Optional[int]  # largest unpruned file scan: a lower bound on the table size
    location_root: Optional[str]
    location_truncated: bool
    # Kudu only: what Spark's byte counters miss. ``rows`` is counted; the bytes are a guess.
    rows: Optional[int] = None
    rows_source: Optional[str] = None   # kudu.rows_read | kudu.num_* (summed)
    columns: Optional[int] = None       # projected by the scans, else the relation's
    bytes_guess: Optional[int] = None   # rows x columns x GUESS_BYTES_PER_COLUMN
    guessed: bool = False
    guess_basis: Optional[str] = None


@dataclass
class AppCoverage:
    """How much of an application's I/O is attributed to a named table or path.

    ``input_bytes`` is the task-level ``bytesRead`` of all stages, the same total
    as ``measurement_summary``'s ``spark_total_input_read_gb`` (in bytes, not GiB).
    ``input_bytes_attributed`` is the part read by stages that ran at least one
    recognised scan; the rest came from sources this module does not name (RDD
    input, streams, caches)."""
    app_id: str
    input_bytes: int
    input_bytes_attributed: int
    output_bytes: int
    tables_read: int
    tables_written: int
    unresolved_scans: int
    unresolved_writes: int
    # Kudu and Kafka I/O, null for an app with none: counted rows, guessed bytes,
    # and every source and target, a Kafka one with topic and rows null.
    unmeasured: Optional[dict] = None


def _value(state: TableAccessState, acc_id: Optional[int]) -> Optional[int]:
    if acc_id is None:
        return None
    if acc_id in state.driver_acc:
        return state.driver_acc[acc_id]
    by_stage = state.stage_acc.get(acc_id)
    return sum(by_stage.values()) if by_stage else None


def _scan_stages(state: TableAccessState, scan: _Scan) -> set[int]:
    stages: set[int] = set()
    for aid in scan.acc.values():
        stages.update(state.stage_acc.get(aid, {}))
    return stages


def build(state: TableAccessState) -> tuple[list[TableAccessRow], AppCoverage]:
    # Bytes of scans that carry no file size: each stage's input, split across the
    # scans it ran in proportion to their output rows.
    stage_scans: dict[int, list[str]] = defaultdict(list)
    for key, scan in state.scans.items():
        for sid in _scan_stages(state, scan):
            stage_scans[sid].append(key)
    stage_bytes: dict[str, float] = defaultdict(float)
    for sid, keys in stage_scans.items():
        total = state.stage_input.get(sid, 0)
        if not total:
            continue
        rows = {k: state.stage_acc.get(state.scans[k].acc.get(_ROWS, -1), {}).get(sid, 0) for k in keys}
        weight = sum(rows.values())
        for k in keys:
            stage_bytes[k] += total * (rows[k] / weight if weight else 1 / len(keys))

    groups: dict[tuple, list] = defaultdict(list)
    unresolved_scans = 0
    for key, scan in state.scans.items():
        if scan.table is None and scan.root is None:
            unresolved_scans += 1
            continue
        size = _value(state, scan.acc.get(_SIZE))
        if size:
            nbytes, source = size, "files"
        elif key in stage_bytes:
            nbytes, source = int(round(stage_bytes[key])), "stage"
        else:
            nbytes, source = None, None
        ident = (scan.table, None if scan.table else scan.root, "read")
        groups[ident].append(dict(
            storage=scan.storage, format=scan.format, bytes=nbytes, source=source,
            files=_value(state, scan.acc.get(_FILES)), partitions=_value(state, scan.acc.get(_PARTITIONS)),
            full=size if (scan.full_scan and size) else None, root=scan.root, truncated=scan.truncated))

    # Writes without a byte metric share their execution's output bytes.
    unmetered: dict[int, list[str]] = defaultdict(list)
    for key, w in state.writes.items():
        if _value(state, w.acc.get(_WRITTEN_BYTES)) is None:
            unmetered[w.execution].append(key)
    exec_output: dict[int, int] = defaultdict(int)
    for sid, ex in state.stage_execution.items():
        exec_output[ex] += state.stage_output.get(sid, 0)
    unresolved_writes = state.unresolved_writes
    for key, w in state.writes.items():
        if w.table is None and w.root is None:
            unresolved_writes += 1
            continue
        metered = _value(state, w.acc.get(_WRITTEN_BYTES))
        if metered is not None:
            nbytes, source = metered, "metric"
        elif exec_output.get(w.execution):
            nbytes, source = exec_output[w.execution] // len(unmetered[w.execution]), "stage"
        else:
            nbytes, source = None, None
        ident = (w.table, None if w.table else w.root, "write")
        groups[ident].append(dict(
            storage=w.storage, format=w.format, bytes=nbytes, source=source,
            files=_value(state, w.acc.get(_WRITTEN_FILES)), partitions=None, full=None,
            root=w.root, truncated=False))

    rows: list[TableAccessRow] = []
    for (table, root_key, direction), ops in groups.items():
        known = [o["bytes"] for o in ops if o["bytes"] is not None]
        sources = {o["source"] for o in ops if o["source"]}
        roots = [o["root"] for o in ops if o["root"]]
        files = [o["files"] for o in ops if o["files"] is not None]
        parts = [o["partitions"] for o in ops if o["partitions"] is not None]
        fulls = [o["full"] for o in ops if o["full"] is not None]
        rows.append(TableAccessRow(
            app_id=state.app_id, table=table, direction=direction,
            storage=Counter(o["storage"] for o in ops).most_common(1)[0][0],
            format=Counter(o["format"] for o in ops).most_common(1)[0][0],
            bytes=sum(known) if known else None,
            bytes_source=(sources.pop() if len(sources) == 1 else "mixed") if sources else None,
            files=sum(files) if files else None, partitions=sum(parts) if parts else None,
            operations=len(ops), full_scan_bytes_max=max(fulls) if fulls else None,
            location_root=Counter(roots).most_common(1)[0][0] if roots else root_key,
            location_truncated=bool(roots) and all(o["truncated"] for o in ops if o["root"]),
        ))

    unmeasured = _unmeasured(state, stage_scans, rows)

    attributed_stages = {sid for sid, keys in stage_scans.items()
                         if any(state.scans[k].table or state.scans[k].root for k in keys)}
    coverage = AppCoverage(
        app_id=state.app_id,
        input_bytes=sum(state.stage_input.values()),
        input_bytes_attributed=sum(state.stage_input.get(s, 0) for s in attributed_stages),
        output_bytes=sum(state.stage_output.values()),
        tables_read=sum(1 for r in rows if r.direction == "read"),
        tables_written=sum(1 for r in rows if r.direction == "write"),
        unresolved_scans=unresolved_scans,
        unresolved_writes=unresolved_writes,
        unmeasured=unmeasured,
    )
    return rows, coverage


def _unmeasured(state: TableAccessState, stage_scans: dict[int, list[str]],
                rows: list[TableAccessRow]) -> Optional[dict]:
    """Kudu rows and guessed bytes on the rows (adding rows for tables seen only in
    a view's plan), and the app's sources and targets, Kafka included."""
    kafka_out = bool(state.kafka_sink_stages)
    kafka_in = bool(state.kafka_source_stages)
    if not (state.kudu_rows_read or state.kudu_rows_written or state.kudu_relation_columns or kafka_out or kafka_in
            or any(s.storage == "kudu" for s in state.scans.values())):
        return None

    # Columns per table: what the physical scans project, else the relation's.
    columns: dict[str, tuple[int, str]] = {t: (n, "relation") for t, n in state.kudu_relation_columns.items() if n}
    for s in state.scans.values():
        if s.storage == "kudu" and s.table and s.columns:
            prior = columns.get(s.table)
            if prior is None or prior[1] == "relation" or s.columns > prior[0]:
                columns[s.table] = (s.columns, "projected")

    # Rows read: a stage's count goes to the Kudu scans that ran in it, by their
    # output rows; a stage with none (a view run through javaRDD) splits its count
    # across the app's Kudu tables by column count, and says so.
    per_stage: dict[int, int] = defaultdict(int)
    for sid, value in state.kudu_rows_read.values():
        per_stage[sid] += value
    read: dict[str, float] = defaultdict(float)
    split: set[str] = set()
    unattributed = 0
    for sid, total in per_stage.items():
        keys = [k for k in stage_scans.get(sid, []) if state.scans[k].storage == "kudu" and state.scans[k].table]
        if keys:
            out = {k: state.stage_acc.get(state.scans[k].acc.get(_ROWS, -1), {}).get(sid, 0) for k in keys}
            weight = sum(out.values())
            for k in keys:
                read[state.scans[k].table] += total * (out[k] / weight if weight else 1 / len(keys))
            continue
        tables = sorted(columns) or sorted({s.table for s in state.scans.values() if s.storage == "kudu" and s.table})
        if not tables:
            unattributed += total
            continue
        cols = {t: columns.get(t, (1, ""))[0] for t in tables}
        for t in tables:
            read[t] += total * cols[t] / sum(cols.values())
        if len(tables) > 1:
            split.update(tables)
    written: dict[str, int] = defaultdict(int)
    for (_, table), n in state.kudu_rows_written.items():
        written[table] += n

    def guess(table: str, n: Optional[int], direction: str) -> tuple[Optional[int], Optional[int], str]:
        """bytes_guess, columns and guess_basis of one row."""
        note = "; rows split across the app's Kudu tables by column count" if direction == "read" and table in split else ""
        if n is None:
            return None, None, "rows not counted"
        c, kind = columns.get(table, (None, ""))
        if kind != "projected":
            return None, None, ("rows counted; no Kudu scan in the plans shows which columns it returns, "
                                "so no byte guess" + note)
        return (n * c * GUESS_BYTES_PER_COLUMN, c,
                f"rows x {c} projected columns x {GUESS_BYTES_PER_COLUMN} B; column types are not in the event log" + note)

    by_key = {(r.table, r.direction): r for r in rows if r.storage == "kudu"}
    for direction, counted in (("read", read), ("write", written)):
        names = set(counted) | ({t for t in columns} if direction == "read" else set())
        for table in sorted(names):
            row = by_key.get((table, direction))
            if row is None:
                row = TableAccessRow(
                    app_id=state.app_id, table=table, direction=direction, storage="kudu", format="kudu",
                    bytes=None, bytes_source=None, files=None, partitions=None, operations=0,
                    full_scan_bytes_max=None, location_root=None, location_truncated=False)
                rows.append(row)
            n = int(round(counted[table])) if table in counted else None
            row.rows = n
            row.rows_source = (_KUDU_ROWS_READ if direction == "read" else "kudu.num_*") if n is not None else None
            row.bytes_guess, row.columns, row.guess_basis = guess(table, n, direction)
            row.guessed = True

    kudu = [r for r in rows if r.storage == "kudu" and r.guessed]
    sources = [dict(storage="kudu", name=r.table, rows=r.rows, bytes_guess=r.bytes_guess)
               for r in kudu if r.direction == "read"]
    targets = [dict(storage="kudu", name=r.table, rows=r.rows, bytes_guess=r.bytes_guess)
               for r in kudu if r.direction == "write"]
    if kafka_in:
        sources.append(dict(storage="kafka", name=None, rows=None, bytes_guess=None))
    if kafka_out:
        targets.append(dict(storage="kafka", name=None, rows=None, bytes_guess=None))
    return dict(
        guessed=True,
        bytes_per_column=GUESS_BYTES_PER_COLUMN,
        rows_read=sum(int(round(v)) for v in read.values()) + unattributed,
        rows_written=sum(written.values()),
        rows_read_unattributed=unattributed,
        bytes_read_guess=sum(r.bytes_guess or 0 for r in kudu if r.direction == "read"),
        bytes_written_guess=sum(r.bytes_guess or 0 for r in kudu if r.direction == "write"),
        kafka_read=kafka_in,
        kafka_write=kafka_out,
        sources=sources,
        targets=targets,
    )


def parse_events(events: Iterable[dict]) -> tuple[list[TableAccessRow], AppCoverage]:
    state = TableAccessState()
    execution_scans: dict[int, set[str]] = {}
    for e in events:
        if isinstance(e, dict):
            process_event(e, state, execution_scans)
    _resolve_row_level_targets(state, execution_scans)
    return build(state)


def _iter_log(zip_path: Path) -> Iterable[dict]:
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        if not names:
            return
        with zf.open(names[0]) as f:
            for line in f:
                head = line[:120]
                if not any(w in head for w in _WANTED):
                    continue
                try:
                    yield json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue


def parse_log(zip_path: Path) -> Optional[tuple[list[TableAccessRow], AppCoverage]]:
    try:
        rows, cov = parse_events(_iter_log(zip_path))
    except Exception as e:  # one broken log must not lose the week
        print(f"  SKIP {zip_path.name}: {e}", file=sys.stderr)
        return None
    return (rows, cov) if cov.app_id else None


# ── Inventory ──────────────────────────────────────────────────────────────────


def build_inventory(rows: list[dict]) -> list[dict]:
    """One row per distinct table. A bare-path access is folded into the table
    whose location contains it; one no table claims stays a ``path:`` entry."""
    roots: dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        if r["table"] and r["location_root"] and not r["location_truncated"]:
            roots[r["table"]][r["location_root"]] += 1
    table_root = {t: c.most_common(1)[0][0] for t, c in roots.items()}
    by_root = sorted(((root, t) for t, root in table_root.items()), key=lambda x: -len(x[0]))

    def owner(path: str) -> Optional[str]:
        for root, t in by_root:
            if path == root or path.startswith(root.rstrip("/") + "/"):
                return t
        return None

    inv: dict[str, dict] = {}
    for r in rows:
        name = r["table"] or (owner(r["location_root"]) if r["location_root"] else None) \
            or f"path:{r['location_root']}"
        r["inventory_table"] = name
        e = inv.setdefault(name, dict(
            table=name, storage=r["storage"], formats=Counter(), location_root=None,
            location_truncated=None, apps_read=set(), apps_written=set(),
            bytes_read=0, bytes_written=0, full_scan_bytes_max=None))
        e["formats"][r["format"]] += 1
        if r["direction"] == "read":
            e["apps_read"].add(r["app_id"])
            e["bytes_read"] += r["bytes"] or 0
        else:
            e["apps_written"].add(r["app_id"])
            e["bytes_written"] += r["bytes"] or 0
        if r["full_scan_bytes_max"]:
            e["full_scan_bytes_max"] = max(e["full_scan_bytes_max"] or 0, r["full_scan_bytes_max"])
    for name, e in inv.items():
        if name in table_root:
            e["location_root"], e["location_truncated"] = table_root[name], False
        elif name.startswith("path:"):
            e["location_root"], e["location_truncated"] = name[len("path:"):], False
        else:
            seen = [r["location_root"] for r in rows if r["inventory_table"] == name and r["location_root"]]
            e["location_root"] = seen[0] if seen else None
            e["location_truncated"] = bool(seen)
        e["formats"] = [f for f, _ in e["formats"].most_common()]
        e["apps_read"], e["apps_written"] = len(e["apps_read"]), len(e["apps_written"])
    return sorted(inv.values(), key=lambda e: -(e["bytes_read"] + e["bytes_written"]))


# ── CLI ────────────────────────────────────────────────────────────────────────


def _parse_one(path: str):
    out = parse_log(Path(path))
    if out is None:
        return None
    rows, cov = out
    return [asdict(r) for r in rows], asdict(cov)


def main() -> None:
    import argparse
    from concurrent.futures import ProcessPoolExecutor

    p = argparse.ArgumentParser(description="Which tables each Spark application reads and writes")
    p.add_argument("--input", type=Path, required=True, help="directory of *.zip event logs")
    p.add_argument("--output", type=Path, required=True, help="directory to write the three JSON files to")
    p.add_argument("--workers", type=int, default=4)
    args = p.parse_args()
    if not args.input.is_dir():
        sys.exit(f"Error: input directory not found: {args.input}")
    logs = sorted(args.input.glob("*.zip"))
    print(f"Reading table access from {len(logs)} event logs in {args.input}", file=sys.stderr)

    rows: list[dict] = []
    coverage: list[dict] = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for i, out in enumerate(pool.map(_parse_one, map(str, logs), chunksize=16), 1):
            if out is not None:
                rows.extend(out[0])
                coverage.append(out[1])
            if i % 500 == 0 or i == len(logs):
                print(f"  Progress: {i}/{len(logs)}", file=sys.stderr)

    inventory = build_inventory(rows)
    args.output.mkdir(parents=True, exist_ok=True)
    for name, data in (("table_access.json", rows), ("table_access_apps.json", coverage),
                       ("table_inventory.json", inventory)):
        with open(args.output / name, "w") as f:
            json.dump(data, f, indent=1)

    total = sum(c["input_bytes"] for c in coverage)
    attributed = sum(c["input_bytes_attributed"] for c in coverage)
    with_table = sum(1 for c in coverage if c["tables_read"] or c["tables_written"])
    print(f"Wrote {len(rows)} access rows, {len(inventory)} tables, {len(coverage)} apps to {args.output}",
          file=sys.stderr)
    print(f"  apps with a table: {with_table}/{len(coverage)}; input bytes attributed: "
          f"{attributed / total:.1%}" if total else "  no input bytes", file=sys.stderr)


if __name__ == "__main__":
    main()
