import json
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.collection.analysis.plan_divergence import (
    Execution,
    Run,
    read_log,
    shape_of,
    to_records,
)
from src.collection.spark.run_splitter import Sidecar

_SQL_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
_SQL_ADAPTIVE = "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"


# --- plan tree builders ----------------------------------------------------


def _node(name, children=(), simple=None):
    return {"nodeName": name, "simpleString": simple if simple is not None else name,
            "children": list(children), "metadata": {}, "metrics": []}


# Two shuffles under a broadcast join: the shape a submitted plan has before AQE
# has seen any runtime statistic.
def _submitted_plan():
    return _node("AdaptiveSparkPlan", [
        _node("SortMergeJoin", [
            _node("Sort", [_node("Exchange", [_node("Scan a")],
                                 simple="Exchange hashpartitioning(a#1, 200)")]),
            _node("Sort", [_node("Exchange", [_node("Scan b")],
                                 simple="Exchange hashpartitioning(b#2, 200)")]),
        ]),
    ], simple="AdaptiveSparkPlan isFinalPlan=false")


# The same query after AQE ran it: both shuffles materialised, so each Exchange is
# wrapped in a ShuffleQueryStage and the tree grows the codegen nodes. The shuffle
# count must not move, because nothing was re-planned.
def _executed_plan_same_shuffles():
    return _node("AdaptiveSparkPlan", [
        _node("WholeStageCodegen (3)", [
            _node("InputAdapter", [
                _node("SortMergeJoin", [
                    _node("ShuffleQueryStage 0", [
                        _node("AQEShuffleRead", [
                            _node("Exchange", [_node("ColumnarToRow", [_node("Scan a")])],
                                  simple="Exchange hashpartitioning(a#1, 200)")],
                            simple="AQEShuffleRead coalesced")]),
                    _node("ShuffleQueryStage 1", [
                        _node("AQEShuffleRead", [
                            _node("Exchange", [_node("ColumnarToRow", [_node("Scan b")])],
                                  simple="Exchange hashpartitioning(b#2, 200)")],
                            simple="AQEShuffleRead coalesced")]),
                ]),
            ]),
        ]),
    ], simple="AdaptiveSparkPlan isFinalPlan=true")


# AQE turned the sort merge join into a broadcast hash join before either exchange
# materialised: two shuffles gone, one broadcast added.
def _executed_plan_converted():
    return _node("AdaptiveSparkPlan", [
        _node("BroadcastHashJoin", [
            _node("ColumnarToRow", [_node("Scan a")]),
            _node("BroadcastQueryStage 0", [
                _node("BroadcastExchange", [_node("ColumnarToRow", [_node("Scan b")])]),
            ]),
        ]),
    ], simple="AdaptiveSparkPlan isFinalPlan=true")


# --- shape_of --------------------------------------------------------------


def test_only_exchange_nodes_count_as_shuffles():
    shape = shape_of(_submitted_plan())
    assert shape.shuffles == 2
    assert shape.broadcasts == 0
    assert shape.reused_exchanges == 0


def test_broadcast_and_reused_exchanges_are_not_shuffles():
    plan = _node("AdaptiveSparkPlan", [
        _node("BroadcastExchange", [_node("Scan a")]),
        _node("ReusedExchange", [], simple="ReusedExchange [d_date_sk#233]"),
        _node("Exchange", [_node("Scan b")], simple="Exchange SinglePartition"),
    ])
    shape = shape_of(plan)
    assert shape.shuffles == 1, "a broadcast runs as its own job, a reuse adds no stage"
    assert shape.broadcasts == 1
    assert shape.reused_exchanges == 1


def test_materialisation_wrappers_do_not_change_the_shuffle_count():
    """The claim the whole module rests on: an executed plan that was not re-planned
    has the same shuffle count as the submitted one, even though it has far more nodes.
    """
    submitted = shape_of(_submitted_plan())
    executed = shape_of(_executed_plan_same_shuffles())
    assert executed.shuffles == submitted.shuffles == 2
    assert executed.nodes > submitted.nodes, "the wrappers are real, they just do not count"
    assert executed.aqe_shuffle_reads == 2


def test_join_conversion_shows_up_as_lost_shuffles():
    submitted = shape_of(_submitted_plan())
    executed = shape_of(_executed_plan_converted())
    assert submitted.shuffles - executed.shuffles == 2
    assert executed.broadcasts == 1


def test_skew_reads_are_counted_only_when_the_node_says_skew():
    plan = _node("AdaptiveSparkPlan", [
        _node("AQEShuffleRead", [], simple="AQEShuffleRead coalesced"),
        _node("AQEShuffleRead", [], simple="AQEShuffleRead local, skewed"),
    ])
    shape = shape_of(plan)
    assert shape.aqe_shuffle_reads == 2
    assert shape.skew_reads == 1


# --- read_log --------------------------------------------------------------


def _write_log(tmp_path: Path, events, name="application_1_1") -> Path:
    zip_path = tmp_path / f"{name}.log.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(name, "\n".join(json.dumps(e) for e in events))
    return zip_path


def _start(exec_id, plan, text="== Parsed Logical Plan ==\n== Physical Plan ==\n"):
    return {"Event": _SQL_START, "executionId": exec_id, "time": 1_000,
            "physicalPlanDescription": text, "sparkPlanInfo": plan}


def _update(exec_id, plan):
    return {"Event": _SQL_ADAPTIVE, "executionId": exec_id,
            "physicalPlanDescription": "== Physical Plan ==\n", "sparkPlanInfo": plan}


def _job(exec_id, stage_ids):
    return {"Event": "SparkListenerJobStart", "Job ID": 0, "Stage IDs": list(stage_ids),
            "Properties": {"spark.sql.execution.id": str(exec_id)}}


def test_submitted_is_the_first_plan_and_executed_is_the_last(tmp_path):
    log = _write_log(tmp_path, [
        _start(1, _submitted_plan()),
        _update(1, _executed_plan_same_shuffles()),
        _update(1, _executed_plan_converted()),
    ])
    (execution,) = read_log(log)
    assert execution.submitted.shuffles == 2
    assert execution.executed.shuffles == 0, "the LAST re-plan is what ran"
    assert execution.replans == 2
    assert execution.drifted


def test_an_execution_with_no_replan_keeps_its_submitted_plan(tmp_path):
    """Dropping these would bias the drift rate upwards by removing the agreeing ones."""
    log = _write_log(tmp_path, [_start(1, _submitted_plan())])
    (execution,) = read_log(log)
    assert execution.replans == 0
    assert execution.executed is execution.submitted
    assert not execution.drifted


def test_plan_sections_and_cardinality_come_from_the_plan_text(tmp_path):
    log = _write_log(tmp_path, [
        _start(1, _submitted_plan(),
               text="== Parsed Logical Plan ==\n== Analyzed Logical Plan ==\n"
                    "== Optimized Logical Plan ==\n== Physical Plan ==\n"),
        _start(2, _submitted_plan(),
               text="== Physical Plan ==\nRelation, Statistics(sizeInBytes=8.0 EiB)\n"),
    ])
    first, second = read_log(log)
    assert first.plan_sections == ["Parsed Logical Plan", "Analyzed Logical Plan",
                                   "Optimized Logical Plan", "Physical Plan"]
    assert first.cardinality_hits == 0
    assert second.cardinality_hits == 2, "sizeInBytes and Statistics( both hit"


def test_the_sidecar_names_an_execution_through_its_job_stages(tmp_path):
    log = _write_log(tmp_path, [
        _start(1, _submitted_plan()),
        _job(1, [4, 5]),
        _start(2, _submitted_plan()),
        _job(2, [9]),
    ])
    sidecar = Sidecar.from_dict({"by_stage": {"4": "query1", "5": "query1", "9": "query2"}})
    first, second = read_log(log, sidecar)
    assert first.query_id == "query1"
    assert second.query_id == "query2"
    assert first.stage_ids == {4, 5}


def test_an_execution_with_no_stages_stays_unnamed(tmp_path):
    """The setup execution (SetCatalogAndNamespace) runs no job, so nothing names it."""
    log = _write_log(tmp_path, [_start(0, _node("SetCatalogAndNamespace"))])
    sidecar = Sidecar.from_dict({"by_stage": {"4": "query1"}})
    (execution,) = read_log(log, sidecar)
    assert execution.query_id is None
    assert Run(label="c", executions=[execution]).named() == []


def test_a_malformed_line_is_skipped_not_fatal(tmp_path):
    zip_path = tmp_path / "application_1_1.log.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("application_1_1", "\n".join([
            "{not json",
            json.dumps(_start(1, _submitted_plan())),
        ]))
    (execution,) = read_log(zip_path)
    assert execution.submitted.shuffles == 2


# --- records ---------------------------------------------------------------


def test_records_carry_the_cell_constants_and_both_counts():
    execution = Execution(exec_id=1, submitted=shape_of(_submitted_plan()),
                          executed=shape_of(_executed_plan_converted()),
                          replans=1, query_id="query1")
    run = Run(label="bw25-rtt0", executions=[execution], max_bw_gbs=25.0, latency_ms=0.434)
    (row,) = to_records([run])
    assert row["cell"] == "bw25-rtt0"
    assert row["tsg_max_bw_gbs"] == 25.0
    assert row["network_latency_ms"] == 0.434
    assert row["submitted_shuffles"] == 2
    assert row["executed_shuffles"] == 0
