import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.analysis.baseline_comparison import compare, published_baseline
from src.collection.spark.model_parser import EnvConstants, _State, build_results, process_event

_FULL = dict(
    window_max_bytes=134217728, mtu=9001, network_latency_ms=0.179, tsg_max_bw_gbs=999.0,
    data_size_gb=325.86, file_format="parquet", compression_codec="uncompressed",
    num_workers=3, worker_disk_bw_gbps=5.0, worker_net_bw_gbps=6.25,
    tsg_avg_bw_gbs=0.0, tsg_transfer_gb=0.0,
)


# --- EnvConstants ----------------------------------------------------------


def test_nothing_supplied_means_nothing_is_claimed():
    env = EnvConstants()
    assert set(env.missing()) == {"window_max_bytes", "mtu", "network_latency_ms",
                                  "tsg_max_bw_gbs"}
    assert env.missing_campaign()
    assert env.total_worker_net_bw_gbps() is None
    # An unset storage configuration reads as unknown, never as "not parquet".
    assert set(env.one_hot().values()) == {None}


def test_a_full_set_reports_nothing_missing():
    env = EnvConstants(**_FULL)
    assert env.missing() == []
    assert env.missing_campaign() == []


def test_the_aggregate_bandwidths_are_derived_from_the_worker_count():
    env = EnvConstants(num_workers=3, worker_disk_bw_gbps=5.0, worker_net_bw_gbps=6.25)
    assert env.total_worker_disk_bw_gbps() == 15.0
    assert env.total_worker_net_bw_gbps() == 18.75


def test_the_storage_flags_use_the_published_one_hot_encoding():
    flags = EnvConstants(file_format="parquet", compression_codec="uncompressed").one_hot()
    assert flags["file_format_parquet"] == 1
    assert flags["file_format_orc"] == 0
    assert flags["compression_codec_uncompressed"] == 1
    assert flags["compression_codec_snappy"] == 0
    assert flags["compression_codec_gzip"] == 0


def test_a_misspelt_storage_configuration_is_refused_at_construction():
    with pytest.raises(ValueError, match="file_format"):
        EnvConstants(file_format="parkay")
    with pytest.raises(ValueError, match="compression_codec"):
        EnvConstants(compression_codec="zstd")


def test_zero_is_a_supplied_value_not_a_missing_one():
    """0.0 is the true interconnect use of an on-premises baseline."""
    env = EnvConstants(tsg_avg_bw_gbs=0.0, tsg_transfer_gb=0.0)
    assert "tsg_avg_bw_gbs" not in env.missing_campaign()
    assert "tsg_transfer_gb" not in env.missing_campaign()


# --- the constants reach the records ---------------------------------------


def _one_stage_state():
    state = _State()
    for event in (
        {"Event": "SparkListenerApplicationStart", "App Name": "a",
         "App ID": "application_1_1", "Timestamp": 1_000_000},
        {"Event": "SparkListenerStageSubmitted", "Stage Info": {"Stage ID": 1},
         "Properties": {"spark.sql.execution.id": 1}},
        {"Event": "SparkListenerStageCompleted", "Stage Info": {
            "Stage ID": 1, "Number of Tasks": 2, "Submission Time": 1_001_000,
            "Completion Time": 1_002_000, "Accumulables": []}},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 1_003_000},
    ):
        process_event(event, state)
    return state


def test_every_constant_lands_on_the_stage_and_job_rows():
    job, stages = build_results(_one_stage_state(), EnvConstants(**_FULL))
    stage = stages[0]
    assert stage.data_size_gb == 325.86
    assert stage.file_format_parquet == 1 and stage.file_format_orc == 0
    assert stage.compression_codec_uncompressed == 1
    assert stage.num_workers == 3
    assert stage.worker_net_bw_gbps == 6.25
    assert stage.total_worker_net_bw_gbps == 18.75
    assert stage.window_max_bytes == 134217728
    assert job.data_size_gb == 325.86
    assert job.tsg_avg_bw_gbs == 0.0
    assert job.file_format_parquet == 1


def test_worker_avg_cpu_stays_null_because_no_event_log_observes_it():
    job, stages = build_results(_one_stage_state(), EnvConstants(**_FULL))
    assert stages[0].worker_avg_cpu is None
    assert job.worker_avg_cpu is None


def test_an_unset_run_keeps_every_constant_null():
    job, stages = build_results(_one_stage_state(), EnvConstants())
    for field in ("data_size_gb", "num_workers", "worker_net_bw_gbps",
                  "total_worker_net_bw_gbps", "file_format_parquet"):
        assert getattr(stages[0], field) is None, field
    assert job.data_size_gb is None


# --- baseline_comparison ---------------------------------------------------


def _published(tmp_path, rows):
    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "raw" / "measurement_summary.json").write_text(json.dumps(rows))
    return tmp_path


def _pub_row(query_id, stages=4, exec_time=10.0, input_gb=1.0):
    return {"query_id": query_id, "tsg_max_bw_gbs": 999.0, "file_format_parquet": 1,
            "file_format_orc": 0, "compression_codec_uncompressed": 1,
            "compression_codec_snappy": 0, "compression_codec_gzip": 0,
            "num_spark_stages": stages, "exec_time_s": exec_time,
            "spark_total_input_read_gb": input_gb, "spark_total_shuffle_read_gb": 0.5,
            "spark_total_executor_runtime_s": 100.0,
            "spark_total_executor_cpu_time_s": 80.0}


def _collected(tmp_path, rows):
    d = tmp_path / "features"
    d.mkdir(parents=True, exist_ok=True)
    (d / "job_features.json").write_text(json.dumps(rows))
    (d / "stage_features.json").write_text(json.dumps(
        [{"query_id": r["query_id"], "unit_kind": "sql_execution"} for r in rows]))
    return d


def test_published_baseline_selects_only_the_matching_configuration(tmp_path):
    shaped = dict(_pub_row("query1"), tsg_max_bw_gbs=10.0)
    orc = dict(_pub_row("query1"), file_format_parquet=0, file_format_orc=1)
    pub = _published(tmp_path, [_pub_row("query1"), shaped, orc])
    rows = published_baseline(pub, "parquet", "uncompressed")
    assert len(rows) == 1


def test_a_faithful_split_passes_even_when_the_runtime_is_faster(tmp_path):
    pub = _published(tmp_path / "pub", [_pub_row("query1"), _pub_row("query2")])
    # Same structure, 30% faster: the runtime changed, the extraction did not.
    faster = [dict(_pub_row(q), exec_time_s=7.0) for q in ("query1", "query2")]
    lines, ok = compare(_collected(tmp_path / "new", faster), pub)
    assert ok, "\n".join(lines)
    assert "PASS" in lines[-1]


def test_a_wrong_split_fails_on_the_structural_columns(tmp_path):
    pub = _published(tmp_path / "pub", [_pub_row("query1"), _pub_row("query2")])
    # Same timings, but each query claims twice the stages: the split is wrong.
    wrong = [dict(_pub_row(q), num_spark_stages=8) for q in ("query1", "query2")]
    lines, ok = compare(_collected(tmp_path / "new", wrong), pub)
    assert not ok
    assert "FAIL" in lines[-1]


def test_an_unsplit_application_is_refused_before_anything_is_compared(tmp_path):
    pub = _published(tmp_path / "pub", [_pub_row("query1")])
    rows = [_pub_row("application_1_1"), _pub_row("application_1_1")]
    lines, ok = compare(_collected(tmp_path / "new", rows), pub)
    assert not ok
    assert "distinct query_id" in lines[0]


# ── Disk geometry: added alongside the published column, never folded into it ──

def test_disk_geometry_is_optional_and_absent_by_default():
    """Existing runs must not become invalid because a new constant exists."""
    env = EnvConstants(num_workers=3, worker_disk_bw_gbps=5.0)
    assert env.worker_disk_count is None
    assert env.worker_disk_bw_per_volume_gbps is None
    assert env.volume_disk_bw_gbps() is None


def test_the_volume_ceiling_is_reported_separately_from_the_published_column():
    """One 250 MB/s volume: the volumes bind, and the published column is unchanged."""
    env = EnvConstants(num_workers=3, worker_disk_bw_gbps=5.0,
                       worker_disk_count=1, worker_disk_bw_per_volume_gbps=2.0)
    assert env.volume_disk_bw_gbps() == 2.0
    assert env.worker_disk_bw_gbps == 5.0


def test_four_volumes_exceed_the_instance_limit_and_the_column_still_stands():
    """Four 250 MB/s volumes offer 8 Gb/s, so the instance link is what binds.

    The point of carrying both: this conclusion is not available from
    worker_disk_bw_gbps alone, and neither number is wrong.
    """
    env = EnvConstants(num_workers=3, worker_disk_bw_gbps=5.0,
                       worker_disk_count=4, worker_disk_bw_per_volume_gbps=2.0)
    assert env.volume_disk_bw_gbps() == 8.0
    assert env.volume_disk_bw_gbps() > env.worker_disk_bw_gbps


def test_a_disk_count_below_one_is_refused():
    with pytest.raises(ValueError, match="worker_disk_count"):
        EnvConstants(worker_disk_count=0)


def test_a_non_positive_per_volume_rate_is_refused():
    with pytest.raises(ValueError, match="per_volume"):
        EnvConstants(worker_disk_bw_per_volume_gbps=0.0)
