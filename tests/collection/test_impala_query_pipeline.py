import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.impala.pipeline import (
    OUTPUT_FILENAME,
    _slim_record,
    process_all,
    process_queries,
    write_output,
)
from src.collection.impala.parser import _NUMERIC_ATTRS


def _raw(query_id, state="FINISHED", duration_ms=1000, statement="select 1"):
    """A raw-shaped CM query dict with a mix of numeric + noise attributes."""
    return {
        "queryId": query_id,
        "queryState": state,
        "durationMillis": duration_ms,
        "startTime": "2026-06-12T00:00:00.000Z",
        "endTime": "2026-06-12T00:00:01.000Z",
        "statement": statement,
        "queryType": "QUERY",
        "database": "default",          # noise — dropped
        "attributes": {
            "hdfs_bytes_read": "2000000000",
            "hdfs_average_scan_range": "1000000",
            "thread_cpu_time": "3000",
            "memory_per_node_peak": "500000000",
            "num_backends": "4",
            "session_id": "abc",         # noise — dropped
            "network_address": "1.2.3.4",  # noise — dropped
            "impala_version": "4.x",     # noise — dropped
        },
    }


def test_slim_record_keeps_numeric_attrs_and_top_level_drops_noise():
    slim = _slim_record(_raw("q1"))
    # Top-level essentials preserved, non-essential top-level dropped.
    assert slim["queryId"] == "q1"
    assert slim["statement"] == "select 1"
    assert "database" not in slim
    # attributes reduced to the _NUMERIC_ATTRS allowlist.
    assert set(slim["attributes"]).issubset(_NUMERIC_ATTRS)
    assert slim["attributes"]["hdfs_bytes_read"] == "2000000000"
    assert slim["attributes"]["memory_per_node_peak"] == "500000000"
    for noise in ("session_id", "network_address", "impala_version"):
        assert noise not in slim["attributes"]


def test_process_queries_filters_non_finished_and_zero_duration():
    seen: set = set()
    kept, skipped, dropped = process_queries([
        _raw("q1"),
        _raw("q2", state="EXCEPTION"),     # dropped: not FINISHED
        _raw("q3", duration_ms=0),         # dropped: no duration
    ], seen)
    assert [r["queryId"] for r in kept] == ["q1"]
    assert skipped == 2
    assert dropped == 0


def test_process_queries_dedups_by_query_id_across_calls():
    seen: set = set()
    kept1, _, _ = process_queries([_raw("q1"), _raw("q2")], seen)
    kept2, _, dropped2 = process_queries([_raw("q2"), _raw("q3")], seen)  # q2 repeats
    assert [r["queryId"] for r in kept1] == ["q1", "q2"]
    assert [r["queryId"] for r in kept2] == ["q3"]
    assert dropped2 == 1


def test_process_all_concatenates_and_writes_consumer_compatible_output(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "impala_a.json").write_text(json.dumps([_raw("q1"), _raw("q2")]))
    (raw_dir / "impala_b.json").write_text(json.dumps([_raw("q2"), _raw("q3")]))  # q2 overlap

    records = process_all(raw_dir)
    assert sorted(r["queryId"] for r in records) == ["q1", "q2", "q3"]

    out_dir = tmp_path / "processed"
    out_path = write_output(records, out_dir)
    assert out_path.name == OUTPUT_FILENAME

    # The output is a plain impala_*.json the ranking loader can read unchanged.
    from src.collection.analysis.ranking import _load_source
    jobs = _load_source(str(out_dir), "impala")
    assert len(jobs) == 3
    assert all(j.start_time == "2026-06-12T00:00:00.000Z" for j in jobs)
