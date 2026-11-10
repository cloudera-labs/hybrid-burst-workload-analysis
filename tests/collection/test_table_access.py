import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.spark.table_access import (
    GUESS_BYTES_PER_COLUMN,
    build_inventory,
    count_columns,
    location_root,
    normalise_table,
    parse_events,
)
from dataclasses import asdict

# Synthetic events mirroring the shapes real event logs carry. Every name is made up.

_SQL_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
_ACCUM = "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates"


def _app(app_id="application_1_1"):
    return {"Event": "SparkListenerApplicationStart", "App ID": app_id, "App Name": "a", "Timestamp": 1}


def _metric(name, acc):
    return {"name": name, "accumulatorId": acc, "metricType": "sum"}


def _file_scan(table, location, partition_filters="[]", accs=(10, 11, 12, 13), fmt="parquet"):
    return {
        "nodeName": f"Scan {fmt} {table}".rstrip(),
        "simpleString": f"FileScan {fmt} {table}[c#1]",
        "children": [],
        "metadata": {"Location": location, "PartitionFilters": partition_filters, "Format": fmt},
        "metrics": [_metric("number of output rows", accs[0]), _metric("size of files read", accs[1]),
                    _metric("number of files read", accs[2]), _metric("number of partitions read", accs[3])],
    }


def _sql(execution, plan, desc=""):
    return {"Event": _SQL_START, "executionId": execution, "sparkPlanInfo": plan,
            "physicalPlanDescription": desc, "description": "q", "time": 1}


def _accum(execution, pairs):
    return {"Event": _ACCUM, "executionId": execution, "accumUpdates": [[a, v] for a, v in pairs]}


def _job(execution, stages):
    return {"Event": "SparkListenerJobStart", "Job ID": 0, "Stage IDs": stages,
            "Properties": {"spark.sql.execution.id": str(execution)}}


def _stage(sid, input_bytes=0, output_bytes=0, accs=()):
    acc = [{"ID": 900 + sid, "Name": "internal.metrics.input.bytesRead", "Value": input_bytes},
           {"ID": 950 + sid, "Name": "internal.metrics.output.bytesWritten", "Value": output_bytes}]
    acc += [{"ID": a, "Name": "number of output rows", "Value": v} for a, v in accs]
    return {"Event": "SparkListenerStageCompleted", "Stage Info": {"Stage ID": sid, "Accumulables": acc}}


def _rows(events):
    rows, cov = parse_events(events)
    return {(r.table, r.direction): r for r in rows}, cov


# --- names and locations ------------------------------------------------------


def test_normalise_table_folds_catalogs_and_quotes():
    assert normalise_table("`Sales`.`Orders`") == "sales.orders"
    assert normalise_table("spark_catalog.sales.orders") == "sales.orders"
    assert normalise_table("impala::sales.orders") == "sales.orders"


def test_location_root_strips_partition_directories():
    root, cut = location_root("InMemoryFileIndex(1 paths)[hdfs://ns/wh/sales.db/orders/dt=2026-10-01/h=3]")
    assert root == "hdfs://ns/wh/sales.db/orders" and not cut


def test_location_root_of_several_partitions_is_their_table():
    root, cut = location_root(
        "InMemoryFileIndex(2 paths)[hdfs://ns/wh/s.db/t/dt=1, hdfs://ns/wh/s.db/t/dt=2]")
    assert root == "hdfs://ns/wh/s.db/t" and not cut


def test_location_root_flags_a_truncated_listing():
    _, cut = location_root("InMemoryFileIndex(5 paths)[hdfs://ns/wh/s.db/t/dt=1, ...]")
    assert cut
    _, cut = location_root("InMemoryFileIndex(1 paths)[hdfs://ns/wh/s.db/t/d...]")
    assert cut


# --- reads --------------------------------------------------------------------


def test_file_scan_takes_its_bytes_from_size_of_files_read():
    plan = _file_scan("sales.orders", "InMemoryFileIndex(1 paths)[hdfs://ns/wh/sales.db/orders/dt=1]",
                      partition_filters="[isnotnull(dt#2)]")
    rows, cov = _rows([_app(), _sql(0, plan), _accum(0, [(11, 5_000), (12, 7), (13, 1)]),
                       _job(0, [1]), _stage(1, input_bytes=4_000, accs=[(10, 100)])])
    r = rows[("sales.orders", "read")]
    assert (r.bytes, r.bytes_source, r.files, r.partitions) == (5_000, "files", 7, 1)
    assert r.location_root == "hdfs://ns/wh/sales.db/orders"
    assert r.full_scan_bytes_max is None  # pruned: not a lower bound on the table
    assert cov.input_bytes == 4_000 and cov.input_bytes_attributed == 4_000


def test_full_scan_is_a_lower_bound_on_table_size():
    plan = _file_scan("sales.orders", "InMemoryFileIndex(1 paths)[hdfs://ns/wh/sales.db/orders]")
    rows, _ = _rows([_app(), _sql(0, plan), _accum(0, [(11, 9_000)])])
    assert rows[("sales.orders", "read")].full_scan_bytes_max == 9_000


def test_adaptive_replan_does_not_double_count_a_scan():
    plan = _file_scan("sales.orders", "InMemoryFileIndex(1 paths)[hdfs://ns/wh/sales.db/orders]")
    update = {"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate",
              "executionId": 0, "sparkPlanInfo": {"nodeName": "AdaptiveSparkPlan", "children": [plan]}}
    rows, _ = _rows([_app(), _sql(0, plan), update, _accum(0, [(11, 9_000)])])
    r = rows[("sales.orders", "read")]
    assert (r.operations, r.bytes) == (1, 9_000)


def test_scans_without_size_share_their_stage_input_by_rows():
    hive = {"nodeName": "Scan hive sales.customers", "simpleString": "Scan hive sales.customers [c#1]",
            "metrics": [_metric("number of output rows", 20)], "children": []}
    ice = {"nodeName": "BatchScan spark_catalog.sales.events", "simpleString": "BatchScan ...",
           "metrics": [_metric("number of output rows", 30)], "children": []}
    plan = {"nodeName": "Union", "children": [hive, ice], "metrics": []}
    rows, cov = _rows([_app(), _sql(0, plan), _job(0, [1]),
                       _stage(1, input_bytes=1_000, accs=[(20, 300), (30, 100)])])
    assert rows[("sales.customers", "read")].bytes == 750
    assert rows[("sales.events", "read")].bytes == 250
    assert rows[("sales.events", "read")].bytes_source == "stage"
    assert cov.input_bytes_attributed == 1_000


def test_kudu_and_jdbc_scans_are_named_with_their_storage():
    kudu = {"nodeName": "Scan Kudu impala::ops.accounts ", "metrics": [_metric("number of output rows", 40)]}
    jdbc = {"nodeName": "Scan JDBCRelation(ledger) [numPartitions=1] ",
            "metrics": [_metric("number of output rows", 41)]}
    rows, _ = _rows([_app(), _sql(0, {"nodeName": "Union", "children": [kudu, jdbc]})])
    assert rows[("ops.accounts", "read")].storage == "kudu"
    assert rows[("jdbc:ledger", "read")].storage == "jdbc"


def test_bare_path_scan_is_kept_by_location_and_claimed_by_its_table():
    named = _file_scan("raw.feed", "InMemoryFileIndex(1 paths)[hdfs://ns/wh/raw.db/feed]", accs=(10, 11, 12, 13))
    bare = _file_scan("", "InMemoryFileIndex(1 paths)[hdfs://ns/wh/raw.db/feed/day=3]",
                      accs=(50, 51, 52, 53), fmt="json")
    other = _file_scan("", "InMemoryFileIndex(1 paths)[hdfs://ns/landing/x]", accs=(60, 61, 62, 63), fmt="json")
    rows, _ = parse_events([_app(), _sql(0, {"nodeName": "Union", "children": [named, bare, other]}),
                            _accum(0, [(11, 10), (51, 20), (61, 30)])])
    bare_rows = [r for r in rows if r.table is None]
    assert {r.location_root for r in bare_rows} == {"hdfs://ns/wh/raw.db/feed", "hdfs://ns/landing/x"}
    inv = {e["table"]: e for e in build_inventory([asdict(r) for r in rows])}
    assert inv["raw.feed"]["bytes_read"] == 30  # the bare path inside its location is the same table
    assert "path:hdfs://ns/landing/x" in inv


def test_rdd_input_stays_unattributed():
    rows, cov = parse_events([_app(), _stage(1, input_bytes=500)])
    assert rows == [] and cov.input_bytes == 500 and cov.input_bytes_attributed == 0


# --- writes -------------------------------------------------------------------


def _write_node(name, simple, accs=(70, 71)):
    return {"nodeName": name, "simpleString": simple,
            "metrics": [_metric("written output", accs[0]), _metric("number of written files", accs[1])]}


def test_insert_into_hive_table():
    node = _write_node("Execute InsertIntoHiveTable",
                       "Execute InsertIntoHiveTable `mart`.`daily`, org.apache.hadoop.hive.ql.io.parquet.serde.X, "
                       "[dt=Some(1)], true, false, [a, b]")
    rows, _ = _rows([_app(), _sql(0, node), _accum(0, [(70, 1_234), (71, 3)])])
    r = rows[("mart.daily", "write")]
    assert (r.bytes, r.bytes_source, r.files, r.storage) == (1_234, "metric", 3, "hdfs")


def test_ctas_commands():
    hive = _write_node("Execute OptimizedCreateHiveTableAsSelectCommand",
                       "Execute OptimizedCreateHiveTableAsSelectCommand [Database: mart, TableName: snap, "
                       "InsertIntoHadoopFsRelationCommand]", accs=(70, 71))
    ds = _write_node("Execute CreateDataSourceTableAsSelectCommand",
                     "Execute CreateDataSourceTableAsSelectCommand `mart`.`copy`, Overwrite, [a]", accs=(72, 73))
    rows, _ = _rows([_app(), _sql(0, {"nodeName": "Union", "children": [hive, ds]})])
    assert ("mart.snap", "write") in rows and ("mart.copy", "write") in rows


def test_insert_into_fs_relation_gives_table_and_location():
    simple = ("Execute InsertIntoHadoopFsRelationCommand hdfs://ns/wh/mart.db/facts/dt=1, [dt=1], false, "
              "Parquet, [path=hdfs://ns/wh/mart.db/facts], Append, CatalogTable(\nDatabase: mart\n"
              "Table: facts\nOwner: x\nType: EXTERNAL\nLocation: hdfs://ns/wh/mart.db/facts\nSerde Library: y\n)")
    rows, _ = _rows([_app(), _sql(0, _write_node("Execute InsertIntoHadoopFsRelationCommand", simple))])
    r = rows[("mart.facts", "write")]
    assert r.location_root == "hdfs://ns/wh/mart.db/facts"


def test_iceberg_writes_take_their_execution_output_bytes():
    append = {"nodeName": "AppendData", "simpleString": "AppendData x@1, IcebergWrite(table=spark_catalog.lake.ev, "
                                                         "format=PARQUET), y", "metrics": []}
    desc = "== Physical Plan ==\n\n(1) ReplaceData\nInput [1]: [a#1]\nArguments: IcebergWrite(table=spark_catalog.lake.st, format=PARQUET)\n\n"
    rows, _ = _rows([_app(), _sql(0, append), _sql(1, {"nodeName": "ReplaceData"}, desc),
                     _job(0, [1]), _stage(1, output_bytes=800), _job(1, [2]), _stage(2, output_bytes=600)])
    assert rows[("lake.ev", "write")].bytes == 800
    assert rows[("lake.st", "write")].bytes == 600
    assert rows[("lake.st", "write")].bytes_source == "stage"


def test_write_delta_target_is_the_one_iceberg_table_it_scans():
    scan = {"nodeName": "BatchScan spark_catalog.lake.st", "metrics": [_metric("number of output rows", 80)]}
    desc = "(1) WriteDelta\nArguments: org.apache.iceberg.spark.source.SparkPositionDeltaWrite@1\n\n"
    rows, cov = _rows([_app(), _sql(0, {"nodeName": "WriteDelta", "children": [scan]}, desc)])
    assert ("lake.st", "write") in rows and cov.unresolved_writes == 0


def test_kudu_write_through_save_into_data_source():
    desc = ("(2) SaveIntoDataSourceCommand\nArguments: org.apache.kudu.spark.kudu.DefaultSource@1, "
            "[kudu.master=m:7051, kudu.table=impala::ops.accounts], Append\n\n")
    rows, _ = _rows([_app(), _sql(0, {"nodeName": "Execute SaveIntoDataSourceCommand"}, desc)])
    assert rows[("ops.accounts", "write")].storage == "kudu"


def test_unnamed_datasource_write_is_counted_unresolved():
    desc = "(1) SaveIntoDataSourceCommand\nArguments: org.apache.spark.X@1, [url=u], Append\n\n"
    rows, cov = _rows([_app(), _sql(0, {"nodeName": "Execute SaveIntoDataSourceCommand"}, desc)])
    assert rows == {} and cov.unresolved_writes == 1


# --- Kudu and Kafka: counted rows, guessed bytes --------------------------------

def _counter_stage(sid, counters, name="stage", details=""):
    acc = [{"ID": 800 + 10 * sid + i, "Name": n, "Value": v} for i, (n, v) in enumerate(counters)]
    return {"Event": "SparkListenerStageCompleted",
            "Stage Info": {"Stage ID": sid, "Stage Name": name, "Details": details, "Accumulables": acc}}


def _view(table, cols, more=0, first=0):
    # Attribute ids are unique in a plan, so two views must not share them.
    listing = ", ".join(f"c{i}#{i}" for i in range(first, first + cols)) + (f", ... {more} more fields" if more else "")
    return f"(3) LogicalRelation\nArguments: Kudu impala::{table}, [{listing}], false\n\n"


def test_count_columns_reads_the_more_fields_note_and_flags_a_cut_list():
    assert count_columns("a#1, b#2, ... 40 more fields") == (42, False)
    assert count_columns("a#1,b#2,c...") == (3, True)


def test_a_kudu_table_seen_only_in_a_view_gets_its_rows_and_a_guess():
    # The pattern of an Envelope job: a view over Kudu, run through javaRDD, so no
    # physical scan and no SQL execution owns the stage that reads.
    events = [_app(), _sql(0, {"nodeName": "Execute CreateViewCommand"}, _view("ops.accounts", 10, more=5)),
              _counter_stage(6, [("kudu.rows_read", "1000"), ("number of output rows", "1000")],
                             name="javaRDD at KafkaOutput.java:80")]
    rows, cov = _rows(events)
    r = rows[("ops.accounts", "read")]
    assert (r.storage, r.bytes, r.operations) == ("kudu", None, 0)  # Spark measured nothing
    assert (r.rows, r.rows_source) == (1000, "kudu.rows_read")
    # A view does not show which columns Kudu returns: rows, but no byte guess.
    assert r.guessed and r.bytes_guess is None and r.columns is None
    assert "no byte guess" in r.guess_basis
    u = cov.unmeasured
    assert u["guessed"] and u["rows_read"] == 1000 and u["bytes_read_guess"] == 0
    assert u["kafka_write"] and not u["kafka_read"]
    assert {"storage": "kafka", "name": None, "rows": None, "bytes_guess": None} in u["targets"]


def test_a_physical_kudu_scan_takes_its_stages_rows_and_its_projected_columns():
    scan = {"nodeName": "Scan Kudu impala::ops.accounts ",
            "simpleString": "Scan Kudu impala::ops.accounts [a#1,b#2,c#3] PushedFilters: []",
            "metrics": [_metric("number of output rows", 40)]}
    events = [_app(), _sql(0, scan, _view("ops.accounts", 30)), _job(0, [1]),
              _counter_stage(1, [("kudu.rows_read", "500"), ("number of output rows", "500")]),
              # The scan's own row metric places it in stage 1.
              _stage(2, accs=()), ]
    events[-2]["Stage Info"]["Accumulables"].append({"ID": 40, "Name": "number of output rows", "Value": 500})
    rows, cov = _rows(events)
    r = rows[("ops.accounts", "read")]
    assert (r.rows, r.columns) == (500, 3)  # projected, not the relation's 30
    assert "projected" in r.guess_basis
    assert r.bytes_guess == 500 * 3 * GUESS_BYTES_PER_COLUMN


def test_rows_with_no_scan_are_split_across_kudu_tables_by_columns_and_say_so():
    desc = _view("ops.big", 30) + _view("ops.small", 10, first=100)
    events = [_app(), _sql(0, {"nodeName": "Execute CreateViewCommand"}, desc),
              _counter_stage(3, [("kudu.rows_read", "400")])]
    rows, cov = _rows(events)
    assert rows[("ops.big", "read")].rows == 300 and rows[("ops.small", "read")].rows == 100
    assert "split" in rows[("ops.big", "read")].guess_basis
    assert rows[("ops.big", "read")].bytes_guess is None
    assert cov.unmeasured["rows_read"] == 400


def test_a_view_counts_only_the_relation_columns_its_plan_uses():
    # Two of five columns are used above the relation; a cut list whose every shown
    # column is used (a select *) counts as all of them. The count only weights a
    # split of rows; it never makes a byte guess.
    from src.collection.spark.table_access import relation_columns_used
    assert relation_columns_used("c0#0, c1#1, c2#2, c3#3, c4#4", "Project [c1#1, c3#3]") == 2
    assert relation_columns_used("c10#10, c11#11, ... 40 more fields", "Project [c10#10, c11#11]") == 42
    assert relation_columns_used("c0#0, c1#1", "nothing else") == 2  # the relation is the view


def test_kudu_write_counters_name_their_tables_and_count_once_per_accumulator():
    events = [_app(), _sql(0, {"nodeName": "Execute CreateViewCommand"}, _view("ops.offsets", 4)),
              _counter_stage(1, [("kudu.num_upserts", "{impala::ops.offsets=2}")]),
              # The same accumulator reported again, larger: its total so far.
              _counter_stage(2, [("kudu.num_upserts", "{impala::ops.offsets=3, impala::ops.other=7}")])]
    events[-1]["Stage Info"]["Accumulables"][0]["ID"] = events[-2]["Stage Info"]["Accumulables"][0]["ID"]
    rows, cov = _rows(events)
    w = rows[("ops.offsets", "write")]
    assert (w.rows, w.rows_source) == (3, "kudu.num_*")
    assert w.bytes_guess is None and "split" not in w.guess_basis
    other = rows[("ops.other", "write")]
    assert other.rows == 7 and other.bytes_guess is None
    assert cov.unmeasured["rows_written"] == 10


def test_an_app_without_kudu_or_kafka_has_no_unmeasured_record():
    plan = _file_scan("sales.orders", "InMemoryFileIndex(1 paths)[hdfs://nn/w/orders]")
    _, cov = _rows([_app(), _sql(0, plan), _accum(0, [(11, 100)])])
    assert cov.unmeasured is None


# --- inventory ----------------------------------------------------------------


def test_inventory_counts_apps_and_bytes_per_table():
    def run(app_id, size):
        plan = _file_scan("sales.orders", "InMemoryFileIndex(1 paths)[hdfs://ns/wh/sales.db/orders]")
        rows, _ = parse_events([_app(app_id), _sql(0, plan), _accum(0, [(11, size)])])
        return [asdict(r) for r in rows]
    inv = build_inventory(run("app_1", 100) + run("app_2", 300))
    e = inv[0]
    assert (e["table"], e["apps_read"], e["bytes_read"], e["full_scan_bytes_max"]) == ("sales.orders", 2, 400, 300)
    assert e["location_root"] == "hdfs://ns/wh/sales.db/orders" and e["location_truncated"] is False
