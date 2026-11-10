import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.analysis.ranking import (
    _group_jobs,
    _load_source,
    _observed_range,
    _parse_spark_measurements,
    _score_jobs,
    _to_json,
    REF_INTENSITY_BPS,
)

_GB = 1024 ** 3


def _job(match_id, app_name, exec_time_s=10.0, active_s=None, input_gb=0.0,
         output_gb=0.0, files=None, cpu_s=0.0, start_time="", end_time="",
         contention_s=0.0):
    return {
        "query_id": app_name, "app_id": app_name, "app_name": app_name,
        "match_id": match_id, "type": "sql", "exec_time_s": exec_time_s,
        "active_exec_time_s": exec_time_s if active_s is None else active_s,
        "spark_total_input_read_gb": input_gb,
        "spark_total_output_write_gb": output_gb,
        "spark_total_executor_cpu_time_s": cpu_s,
        "total_files_read": files,
        "contention_tax_s": contention_s,
        "start_time": start_time,
        "end_time": end_time,
    }


def test_parse_measurements_maps_storage_io_and_excludes_shuffle():
    # Shuffle fields are present but must NOT contribute to bytes; only input+output.
    rec = _job("m_1", "app", exec_time_s=10.0, active_s=3.0, input_gb=2.0,
               output_gb=1.0, files=50, cpu_s=8.0)
    rec["spark_total_shuffle_read_gb"] = 999.0  # noise — must be ignored
    jobs = _parse_spark_measurements([rec])
    assert len(jobs) == 1
    j = jobs[0]
    assert j.bytes_read == 2.0 * _GB
    assert j.bytes_written == 1.0 * _GB
    assert j.file_count == 50
    assert j.duration_s == 10.0                     # wall-clock
    assert j.exec_active_s == 3.0                   # active stage span, below wall-clock
    assert j.cpu_time_ms == 8000
    assert j.signature == "m_1"                     # match_id is the grouping key
    assert j.name == "app"


def test_intensity_score_uses_active_window_not_wall_clock():
    # 3 GB moved over a 3 s active window = 1 GB/s, far above the 125 MB/s reference,
    # so intensity_score caps at 100 (High) — even though wall-clock is 10 s. Proves the
    # scoring rate uses exec_active_s, not duration_s.
    rec = _job("m_1", "app", exec_time_s=10.0, active_s=3.0, input_gb=2.0, output_gb=1.0)
    j = _score_jobs(_parse_spark_measurements([rec]))[0]
    active_rate = (3.0 * _GB) / 3.0
    assert active_rate > REF_INTENSITY_BPS          # 1 GB/s ≫ 125 MB/s
    assert j.intensity_score == 100.0               # capped, driven by the active-window rate
    assert j.risk == "High"
    assert "BW" in j.why


def test_json_intensities_are_emitted_in_bits():
    # JSON "_bps" fields are bits/sec (× 8 vs the internal bytes/sec), and the reference
    # bandwidth is 1 Gbps in bits. 1 GB read over a 2 s active window = 4 Gbit/s.
    rec = _job("m_1", "app", exec_time_s=10.0, active_s=2.0, input_gb=1.0, output_gb=0.0)
    jobs = _score_jobs(_parse_spark_measurements([rec]))
    payload = _to_json(jobs, "spark")
    j = payload["jobs"][0]
    assert j["read_intensity_bps"] == round(1.0 * _GB * 8 / 2.0, 1)   # bytes × 8 / active
    assert payload["reference"]["intensity_bps"] == REF_INTENSITY_BPS * 8 == 1e9


def test_contention_tax_parsed_grouped_and_emitted_for_spark():
    # Two runs of one pattern with executor-acquisition waits of 10 s and 30 s:
    # grouped mean = 20 s, std = 10 s. Emitted as contention_tax_s / _std_s in Spark JSON.
    records = [
        _job("m_1", "app", exec_time_s=20, active_s=15, contention_s=10.0),
        _job("m_1", "app", exec_time_s=20, active_s=15, contention_s=30.0),
    ]
    parsed = _parse_spark_measurements(records)
    assert [round(j.contention_tax_s, 1) for j in parsed] == [10.0, 30.0]
    grouped, _q = _group_jobs(parsed)
    g = grouped[0]
    assert g.runs == 2
    assert round(g.contention_tax_s, 1) == 20.0
    assert round(g.contention_tax_std_s, 1) == 10.0
    j = _to_json(_score_jobs(grouped), "spark", grouped=True)["jobs"][0]
    assert j["contention_tax_s"] == 20.0
    assert j["contention_tax_std_s"] == 10.0


def test_contention_tax_omitted_from_impala_json():
    # Impala has no executor concept, so the contention keys must not appear.
    from src.collection.analysis.ranking import _parse_impala_queries
    q = {"queryId": "q1", "queryState": "FINISHED", "durationMillis": 1000,
         "statement": "select 1", "attributes": {"hdfs_bytes_read": 10}}
    jobs = _score_jobs(_parse_impala_queries([q]))
    j = _to_json(jobs, "impala")["jobs"][0]
    assert "contention_tax_s" not in j
    assert "resources" not in j


def test_impala_wall_clock_splits_into_three_phases():
    # Wall-clock = admission wait + (planning + client fetch) + active, all in ms.
    # resource_wait_s is the admission wait; exec_active_s is the residual.
    from src.collection.analysis.ranking import _parse_impala_queries
    q = {"queryId": "q1", "queryState": "FINISHED", "durationMillis": 100_000,
         "statement": "select 1",
         "attributes": {"admission_wait": 10_000, "planning_wait_time": 5_000,
                        "client_fetch_wait_time": 25_000}}
    [j] = _parse_impala_queries([q])
    assert j.duration_s == 100.0
    assert j.resource_wait_s == 10.0                 # admission wait
    assert j.exec_active_s == 60.0                   # 100 - 10 - 5 - 25
    # Planning/startup is the derived remainder: duration - active - resource_wait.
    assert round(j.duration_s - j.exec_active_s - j.resource_wait_s, 3) == 30.0


def test_impala_active_falls_back_to_duration_without_timing_attrs():
    # A row with no timing counters keeps the old behaviour: active == wall-clock.
    from src.collection.analysis.ranking import _parse_impala_queries
    q = {"queryId": "q1", "queryState": "FINISHED", "durationMillis": 4_000,
         "statement": "select 1", "attributes": {"hdfs_bytes_read": 10}}
    [j] = _parse_impala_queries([q])
    assert j.exec_active_s == 4.0
    assert j.resource_wait_s == 0.0


def test_impala_active_clamped_to_zero_when_counters_overlap():
    # Overlapping counters must not drive active negative.
    from src.collection.analysis.ranking import _parse_impala_queries
    q = {"queryId": "q1", "queryState": "FINISHED", "durationMillis": 1_000,
         "statement": "select 1",
         "attributes": {"admission_wait": 200, "planning_wait_time": 100,
                        "client_fetch_wait_time": 5_000}}
    [j] = _parse_impala_queries([q])
    assert j.exec_active_s == 0.0
    assert j.resource_wait_s == 0.2


def test_parse_measurements_skips_zero_duration_and_null_files():
    jobs = _parse_spark_measurements([
        _job("m_1", "a", exec_time_s=0.0, input_gb=5.0),   # dropped: no duration
        _job("m_2", "b", exec_time_s=5.0, files=None),     # kept: files -> 0
    ])
    assert [j.name for j in jobs] == ["b"]
    assert jobs[0].file_count == 0


def test_group_by_match_id_aggregates_recurring_runs():
    # Two runs of the same match_id, one of another. GROUP collapses to 2 patterns.
    records = [
        _job("hdfs_to_hive_abc12345", "HDFS to Hive", exec_time_s=10, active_s=3, input_gb=1.0, files=4),
        _job("hdfs_to_hive_abc12345", "HDFS to Hive", exec_time_s=10, active_s=4, input_gb=1.0, files=4),
        _job("other_def67890", "Other", exec_time_s=5, input_gb=0.5, files=2),
    ]
    grouped, _q = _group_jobs(_parse_spark_measurements(records))
    by_runs = {g.runs for g in grouped}
    assert len(grouped) == 2
    assert 2 in by_runs and 1 in by_runs
    # The 2-run group reports PER-RUN MEANS (not sums) for files, bytes, wall-clock,
    # and active execution time, plus the std across runs.
    big = max(grouped, key=lambda g: g.runs)
    assert big.file_count == 4                  # mean of [4, 4]
    assert big.bytes_read == 1.0 * _GB          # mean of [1.0, 1.0] GB
    assert big.duration_s == 10.0               # mean of [10, 10]
    assert big.exec_active_s == 3.5             # mean of [3, 4]
    assert big.bytes_read_std == 0.0            # identical reads -> no spread
    assert big.exec_active_std_s == 0.5         # std of [3, 4]


def test_parse_measurements_captures_start_end_times():
    rec = _job("m_1", "app", start_time="2026-06-11T23:55:12.320Z",
               end_time="2026-06-12T00:10:32.653Z")
    j = _parse_spark_measurements([rec])[0]
    assert j.start_time == "2026-06-11T23:55:12.320Z"
    assert j.end_time == "2026-06-12T00:10:32.653Z"


def test_observed_range_spans_min_start_to_max_end():
    jobs = _parse_spark_measurements([
        _job("m_1", "a", start_time="2026-06-11T00:00:00.000Z",
             end_time="2026-06-11T01:00:00.000Z"),
        _job("m_2", "b", start_time="2026-06-29T10:00:00.000Z",
             end_time="2026-06-29T23:46:33.266Z"),
    ])
    assert _observed_range(jobs) == (
        "2026-06-11T00:00:00.000Z", "2026-06-29T23:46:33.266Z")


def test_observed_range_empty_when_no_timestamps():
    jobs = _parse_spark_measurements([_job("m_1", "a")])  # no start/end
    assert _observed_range(jobs) == ("", "")


def test_grouping_carries_earliest_start_and_latest_end():
    grouped, _q = _group_jobs(_parse_spark_measurements([
        _job("m_1", "a", start_time="2026-06-11T00:00:00.000Z",
             end_time="2026-06-11T01:00:00.000Z"),
        _job("m_1", "a", start_time="2026-06-13T00:00:00.000Z",
             end_time="2026-06-13T02:00:00.000Z"),
    ]))
    assert len(grouped) == 1
    assert grouped[0].start_time == "2026-06-11T00:00:00.000Z"
    assert grouped[0].end_time == "2026-06-13T02:00:00.000Z"


def test_load_source_reads_measurement_summary_from_dir(tmp_path):
    d = tmp_path / "processed" / "spark"
    d.mkdir(parents=True)
    (d / "measurement_summary.json").write_text(json.dumps([
        _job("m_1", "app_a", exec_time_s=10, input_gb=1.0, files=3),
    ]))
    jobs = _load_source(str(d), "spark")
    assert len(jobs) == 1
    assert jobs[0].name == "app_a"
    assert jobs[0].file_count == 3


def test_load_source_reads_measurement_summary_file_directly(tmp_path):
    f = tmp_path / "measurement_summary.json"
    f.write_text(json.dumps([_job("m_1", "app_a", exec_time_s=10, input_gb=1.0)]))
    jobs = _load_source(str(f), "spark")
    assert len(jobs) == 1 and jobs[0].name == "app_a"


def _impala_q(query_id, bytes_read=1000):
    return {
        "queryId": query_id,
        "queryState": "FINISHED",
        "durationMillis": 1000,
        "statement": f"select * from t where x = {query_id}",
        "attributes": {"hdfs_bytes_read": bytes_read, "hdfs_bytes_written": 0},
    }


def test_load_source_impala_dir_concatenates_all_files(tmp_path):
    d = tmp_path / "impala"
    d.mkdir()
    # Two non-overlapping downloaded windows.
    (d / "impala_2026-06-12Z_2026-06-19Z.json").write_text(
        json.dumps([_impala_q("a"), _impala_q("b")]))
    (d / "impala_2026-06-19Z_2026-06-25Z.json").write_text(
        json.dumps([_impala_q("c")]))
    jobs = _load_source(str(d), "impala")
    # Full range across both files, not just the latest.
    assert len(jobs) == 3


def test_load_source_impala_dir_dedups_overlapping_queries(tmp_path):
    d = tmp_path / "impala"
    d.mkdir()
    # Overlapping windows share query "b" — it must be counted once.
    (d / "impala_2026-06-12Z_2026-06-20Z.json").write_text(
        json.dumps([_impala_q("a"), _impala_q("b")]))
    (d / "impala_2026-06-18Z_2026-06-25Z.json").write_text(
        json.dumps([_impala_q("b"), _impala_q("c")]))
    jobs = _load_source(str(d), "impala")
    assert len(jobs) == 3
