import re
from dataclasses import dataclass
from typing import Optional


_NUMERIC_ATTRS = {
    "thread_cpu_time", "thread_cpu_time_percentage",
    "thread_network_receive_wait_time", "thread_network_receive_wait_time_percentage",
    "thread_network_send_wait_time", "thread_network_send_wait_time_percentage",
    "thread_storage_wait_time", "thread_storage_wait_time_percentage",
    "thread_total_time",
    "hdfs_bytes_read", "hdfs_bytes_read_local", "hdfs_bytes_read_remote",
    "hdfs_bytes_read_short_circuit", "hdfs_bytes_read_from_cache",
    "hdfs_bytes_read_erasure_coded",
    "hdfs_bytes_read_local_percentage", "hdfs_bytes_read_remote_percentage",
    "hdfs_bytes_read_short_circuit_percentage", "hdfs_bytes_read_from_cache_percentage",
    "hdfs_average_scan_range",
    "hdfs_bytes_written",
    "bytes_streamed",
    "memory_aggregate_peak", "memory_per_node_peak", "estimated_per_node_peak_memory",
    "memory_accrual", "memory_spilled",
    "num_backends", "num_fragments",
    "admission_wait", "planning_wait_time", "planning_wait_time_percentage",
    "client_fetch_wait_time", "client_fetch_wait_time_percentage",
}

# Profile metrics to aggregate (sum) across all operator nodes
_PROFILE_SUM_METRICS = {
    "ScanRangesComplete",
    "RemoteScanRanges",
    "NumRowGroups",
    "NumRowGroupsWithPageIndex",
    "NumDictFilteredRowGroups",
    "NumBloomFilteredRowGroups",
    "NumStatsFilteredRowGroups",
    "NumRuntimeFilteredRowGroups",
    "BytesRead",
    "BytesReadLocal",
    "BytesReadRemoteUnexpected",
    "BytesReadShortCircuit",
    "BytesReadDataNodeCache",
    "RowsRead",
    "RowsReturned",
    "TotalRawHdfsOpenFileTime",
    "TotalRawHdfsReadTime",
    "ScannerIoWaitTime",
    "DecompressionTime",
    "IoReadTotalBytes",
}

# Regex: "          - MetricName: human_value (raw_int)"
_COUNTER_RE = re.compile(r"^\s+-\s+([\w.]+):\s+.*?\((\d+)\)\s*$")

# Query options line pattern
_SCANNER_THREADS_RE = re.compile(r"NUM_SCANNER_THREADS=(\d+)")

# Query Timeline patterns (values are nanoseconds from query start)
_ROWS_AVAILABLE_RE = re.compile(r"Rows available:\s+[\d.]+\w+\s+\((\d+)\)")
_BACKENDS_STARTED_RE = re.compile(r"All \d+ execution backends.*started:\s+[\d.]+\w+\s+\((\d+)\)")

# Plan section patterns for SCAN HDFS nodes
_SCAN_HDFS_PLAN_RE = re.compile(r"(\d+):SCAN HDFS \[([^,\]]+)")
_HDFS_PARTITIONS_RE = re.compile(r"HDFS partitions=(\d+)/(\d+) files=(\d+) size=(\S+)")
_TABLE_STATS_RE = re.compile(r"table:\s+rows=(\S+)\s+size=(\S+)")
_ICEBERG_SNAPSHOT_RE = re.compile(r"Iceberg snapshot id:")

# Execution section patterns for HDFS_SCAN_NODE
_HDFS_NODE_EXEC_RE = re.compile(r"HDFS_SCAN_NODE \(id=(\d+)\)")
_ANY_EXEC_NODE_RE = re.compile(r"^\s+\w+\s*\(id=\d+\)")

# Summary section patterns for parse_profile_attrs
_DURATION_US_RE = re.compile(r"^\s+Duration:\s+[\d.]+\w+\s+\((\d+)\s+us\)")
_QUERY_SUBMITTED_NS_RE = re.compile(r"Query submitted:\s+[\d.]+\w+\s+\((\d+)\)")
_PLANNING_FINISHED_NS_RE = re.compile(r"Planning finished:\s+[\d.]+\w+\s+\((\d+)\)")
# Captures multi-line SQL statement up to the next 4-space summary field
_SQL_STATEMENT_RE = re.compile(
    r"^\s{4}Sql Statement:\s*(.*?)(?=\n\s{4}\w)",
    re.MULTILINE | re.DOTALL,
)


def parse_query_attributes(query: dict) -> dict:
    """Extract numeric model inputs from a Cloudera Manager query export entry."""
    attrs = query.get("attributes", {})
    result: dict = {
        "query_id": query.get("queryId"),
        "duration_ms": query.get("durationMillis"),
        "rows_produced": query.get("rowsProduced"),
        "query_type": query.get("queryType"),
    }
    for key in _NUMERIC_ATTRS:
        raw = attrs.get(key)
        if raw is None:
            continue
        try:
            result[key] = float(raw)
        except (ValueError, TypeError):
            pass
    return result


def parse_profile_txt(text: str) -> dict:
    """
    Aggregate profile counters from Impala's text-format execution profile.

    Sums each metric across all operator nodes and fragment instances.
    Returns raw integer values keyed by metric name, plus 'execution_time_s'
    derived from the Query Timeline (backends started → rows available).
    """
    totals: dict[str, int] = {}
    rows_available_ns: Optional[int] = None
    backends_started_ns: Optional[int] = None

    for line in text.splitlines():
        m = _COUNTER_RE.match(line)
        if m:
            name, raw = m.group(1), int(m.group(2))
            if name in _PROFILE_SUM_METRICS:
                totals[name] = totals.get(name, 0) + raw
            continue

        if rows_available_ns is None:
            m2 = _ROWS_AVAILABLE_RE.search(line)
            if m2:
                rows_available_ns = int(m2.group(1))
                continue

        if backends_started_ns is None:
            m3 = _BACKENDS_STARTED_RE.search(line)
            if m3:
                backends_started_ns = int(m3.group(1))

    if rows_available_ns is not None and backends_started_ns is not None:
        totals["execution_time_s"] = (rows_available_ns - backends_started_ns) / 1e9

    return totals


def parse_profile_attrs(text: str, query_id: str) -> dict:
    """
    Extract query attributes from Impala profile text.

    Used when only the profile text is available (e.g. fetching a single query by ID),
    since the detail endpoint returns only profile text, not JSON metadata.
    """
    attrs: dict = {"query_id": query_id, "query_type": "QUERY"}
    query_submitted_ns: Optional[int] = None
    planning_finished_ns: Optional[int] = None

    for line in text.splitlines():
        if "duration_ms" not in attrs:
            m = _DURATION_US_RE.match(line)
            if m:
                attrs["duration_ms"] = int(m.group(1)) / 1000.0
        if query_submitted_ns is None:
            m = _QUERY_SUBMITTED_NS_RE.search(line)
            if m:
                query_submitted_ns = int(m.group(1))
        if planning_finished_ns is None:
            m = _PLANNING_FINISHED_NS_RE.search(line)
            if m:
                planning_finished_ns = int(m.group(1))
        cm = _COUNTER_RE.match(line)
        if cm:
            name, raw = cm.group(1), int(cm.group(2))
            if name == "NumBackends" and "num_backends" not in attrs:
                attrs["num_backends"] = raw
            elif name == "TotalBytesRead" and "hdfs_bytes_read" not in attrs:
                attrs["hdfs_bytes_read"] = float(raw)
            elif name == "ClientFetchWaitTimer" and "client_fetch_wait_time" not in attrs:
                attrs["client_fetch_wait_time"] = raw / 1e6  # ns → ms

    if query_submitted_ns is not None and planning_finished_ns is not None:
        attrs["planning_wait_time"] = (planning_finished_ns - query_submitted_ns) / 1e6

    m = _SQL_STATEMENT_RE.search(text)
    if m:
        attrs["statement"] = m.group(1).strip()

    return attrs


def parse_sql_statement(text: str) -> Optional[str]:
    """Extract the SQL statement from an Impala text-format profile."""
    m = _SQL_STATEMENT_RE.search(text)
    return m.group(1).strip() if m else None


def parse_scanner_threads(text: str) -> Optional[int]:
    """Extract NUM_SCANNER_THREADS from the query options line in the profile."""
    m = _SCANNER_THREADS_RE.search(text)
    return int(m.group(1)) if m else None


def _parse_size_gb(s: str) -> float:
    """Parse a human-readable size string into GB float. E.g. '3.38GB' → 3.38, '240.15MB' → 0.234."""
    s = s.strip()
    m = re.match(r"^([\d.]+)\s*(TiB|GiB|MiB|KiB|TB|GB|MB|KB|B)$", s, re.IGNORECASE)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2).upper()
    if unit in ("TIB", "TB"):
        return val * 1024.0
    if unit in ("GIB", "GB"):
        return val
    if unit in ("MIB", "MB"):
        return val / 1024.0
    if unit in ("KIB", "KB"):
        return val / (1024.0 * 1024.0)
    if unit == "B":
        return val / (1024.0 * 1024.0 * 1024.0)
    return 0.0


def _parse_rows_float(s: str) -> float:
    """Parse a human-readable row count into a float. E.g. '21.38M' → 2.138e7, '2.71G' → 2.71e9."""
    s = s.strip().replace(",", "")
    m = re.match(r"^([\d.]+)\s*([KMGTkmgt]?)$", s)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2).upper()
    if unit == "K":
        return val * 1e3
    if unit == "M":
        return val * 1e6
    if unit == "G":
        return val * 1e9
    if unit == "T":
        return val * 1e12
    return val


@dataclass
class ScanTableInfo:
    """Per-HDFS-scan-node metadata from plan + execution sections of a profile."""
    node_id: int
    table: str
    partitions_scanned: int
    partitions_total: int
    files: int
    size_after_pruning_gb: float       # plan "HDFS partitions=... size=..."
    table_total_size_gb: float         # plan "table: rows=... size=..."
    table_total_rows_est: float        # plan "table: rows=..." (float, millions/billions)
    is_iceberg: bool = False           # True when "Iceberg snapshot id:" found in plan section
    bytes_read_gb: float = 0.0         # execution BytesRead counter (bytes / 1e9)
    scan_ranges_completed: int = 0     # execution ScanRangesComplete
    rows_read: int = 0                 # execution RowsRead
    num_row_groups: int = 0            # execution NumRowGroups
    remote_scan_ranges: int = 0        # execution RemoteScanRanges


def parse_scan_tables(text: str) -> list[ScanTableInfo]:
    """
    Parse per-table scan metadata from an Impala text-format profile.

    Phase 1: extract plan-section SCAN HDFS nodes (table name, partitions, sizes).
    Phase 2: accumulate execution-section HDFS_SCAN_NODE counters per node ID
             (summed across all fragment instances).
    Phase 3: join on integer node ID and return one ScanTableInfo per plan node.
    """
    lines = text.splitlines()

    # ── Phase 1: plan metadata ──────────────────────────────────────────────────
    plan_nodes: dict[int, dict] = {}
    for i, line in enumerate(lines):
        m = _SCAN_HDFS_PLAN_RE.search(line)
        if not m:
            continue
        node_id = int(m.group(1))
        table = m.group(2).strip()
        info: dict = {
            "table": table,
            "partitions_scanned": 0,
            "partitions_total": 0,
            "files": 0,
            "size_after_pruning_gb": 0.0,
            "table_total_size_gb": 0.0,
            "table_total_rows_est": 0.0,
            "is_iceberg": False,
        }
        found_partitions = False
        found_table_stats = False
        for j in range(i + 1, min(i + 20, len(lines))):
            scan_line = lines[j]
            if not found_partitions:
                m2 = _HDFS_PARTITIONS_RE.search(scan_line)
                if m2:
                    info["partitions_scanned"] = int(m2.group(1))
                    info["partitions_total"] = int(m2.group(2))
                    info["files"] = int(m2.group(3))
                    info["size_after_pruning_gb"] = _parse_size_gb(m2.group(4))
                    found_partitions = True
            if not found_table_stats:
                m3 = _TABLE_STATS_RE.search(scan_line)
                if m3:
                    info["table_total_rows_est"] = _parse_rows_float(m3.group(1))
                    info["table_total_size_gb"] = _parse_size_gb(m3.group(2))
                    found_table_stats = True
            if _ICEBERG_SNAPSHOT_RE.search(scan_line):
                info["is_iceberg"] = True
            if found_partitions and found_table_stats and info["is_iceberg"]:
                break
        plan_nodes[node_id] = info

    # ── Phase 2: execution counters per node ID ─────────────────────────────────
    node_counters: dict[int, dict] = {}
    current_scan_id: Optional[int] = None

    for line in lines:
        if _ANY_EXEC_NODE_RE.match(line):
            hdfs_m = _HDFS_NODE_EXEC_RE.search(line)
            if hdfs_m:
                current_scan_id = int(hdfs_m.group(1))
                if current_scan_id not in node_counters:
                    node_counters[current_scan_id] = {}
            else:
                current_scan_id = None
            continue

        if current_scan_id is not None:
            cm = _COUNTER_RE.match(line)
            if cm:
                name, raw = cm.group(1), int(cm.group(2))
                node_counters[current_scan_id][name] = node_counters[current_scan_id].get(name, 0) + raw

    # ── Phase 3: join plan + execution ─────────────────────────────────────────
    results: list[ScanTableInfo] = []
    for node_id, info in plan_nodes.items():
        counters = node_counters.get(node_id, {})
        results.append(ScanTableInfo(
            node_id=node_id,
            table=info["table"],
            partitions_scanned=info["partitions_scanned"],
            partitions_total=info["partitions_total"],
            files=info["files"],
            size_after_pruning_gb=info["size_after_pruning_gb"],
            table_total_size_gb=info["table_total_size_gb"],
            table_total_rows_est=info["table_total_rows_est"],
            is_iceberg=info["is_iceberg"],
            bytes_read_gb=counters.get("BytesRead", 0) / 1e9,
            scan_ranges_completed=counters.get("ScanRangesComplete", 0),
            rows_read=counters.get("RowsRead", 0),
            num_row_groups=counters.get("NumRowGroups", 0),
            remote_scan_ranges=counters.get("RemoteScanRanges", 0),
        ))
    return results
