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

"""Split one Spark application into the units the model treats as separate runs.

The model's row is a *query*: one job dependency graph, matched to its on-premises
baseline by ``query_id``. In production that is also one Spark application, so the
two coincide and nothing here is needed. A benchmark harness breaks that
assumption: the TPC-DS driver runs all 99 queries inside a single long-lived
application, so an application-level row would be one 3208-second "query" and the
per-query structure the model calibrates on would be gone.

This module supplies the missing axis. A *splitter* cuts the parsed application
state into units, and every unit is then fed through the unchanged
``build_results`` / ``build_graph``:

``none``
    One unit, the whole application. ``query_id`` is the application id. This is
    the production default and reproduces the pre-split output exactly.

``sql``
    One unit per SQL execution that produced stages, cut on
    ``SparkListenerSQLExecutionStart`` / ``End``. Every stage carries
    ``spark.sql.execution.id`` in its submission properties, so the partition is
    read from the log rather than inferred from timing.

Naming is a separate concern from cutting, because the event log does not contain
the benchmark's query names: the TPC-DS driver's ``description`` is
``foreach at SparkSQLExecutor.java:89`` for all 99 queries. Names, and the harness's
own execution timer, therefore come from a side-car (see ``tpcds_sidecar``). Without
one a unit is keyed ``<app_id>#exec<n>`` - honest, joinable, and useless for baseline
matching, which is the correct failure mode: nothing is invented.

Verified against the published campaign. For the 7.3.2 on-premises baseline
(``application_1788681512191_0011``) the ``sql`` splitter recovers 99 units from
1108 stages, and the partition agrees with the 7.3.1 listener's own attribution on
every stage: no stage lands in two queries, and the per-query stage counts match
the published ``num_spark_stages`` with a median ratio of 1.000.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Optional

from .model_parser import _State

SPLIT_NONE = "none"
SPLIT_SQL = "sql"
SPLITTERS = (SPLIT_NONE, SPLIT_SQL)


@dataclass(frozen=True)
class Unit:
    """One modelled run carved out of an application.

    ``query_id`` is the recurrence key the model matches a run to its baseline on.
    ``unit_id`` is the provenance key inside the application (the app id itself for
    ``none``, the SQL execution id for ``sql``); it stays unique even when two units
    share a ``query_id``. ``harness_exec_time_s`` is the benchmark's own timing for
    this unit when a side-car supplied one, and ``None`` otherwise.
    """
    query_id: str
    unit_kind: str
    unit_id: str
    start_ts: int
    end_ts: int
    stage_ids: frozenset[int] = field(default_factory=frozenset)
    labelled: bool = True
    harness_exec_time_s: Optional[float] = None


class Sidecar:
    """What the harness recorded about a run that its event log cannot express.

    Two things, and they are independent: the query *name* of each unit, and the
    harness's own *execution timer* for it. Names accept either grain - ``by_stage``
    (a stage id names its query, what the TPC-DS metrics bundle records) or
    ``by_exec`` (a SQL execution id names its query) - and a unit resolves through
    whichever is present. Timings are keyed by the resolved name, so a unit with no
    name gets no timing either.

    A unit whose stages disagree is reported, not silently resolved by majority:
    disagreement means the side-car and the log describe different runs, and a wrong
    ``query_id`` corrupts the baseline join.
    """

    def __init__(self, by_stage: Optional[dict[int, str]] = None,
                 by_exec: Optional[dict[int, str]] = None,
                 exec_time_s: Optional[dict[str, float]] = None) -> None:
        self.by_stage = dict(by_stage or {})
        self.by_exec = dict(by_exec or {})
        self.exec_time_s = dict(exec_time_s or {})
        self.conflicts: list[str] = []

    def __bool__(self) -> bool:
        return bool(self.by_stage or self.by_exec or self.exec_time_s)

    @classmethod
    def from_dict(cls, raw: dict) -> "Sidecar":
        """Read ``{"by_stage"|"by_exec"|"exec_time_s": {...}}``, or a bare stage map."""
        def _names(d) -> dict[int, str]:
            out: dict[int, str] = {}
            for k, v in (d or {}).items():
                try:
                    out[int(k)] = str(v)
                except (ValueError, TypeError):
                    continue
            return out

        def _times(d) -> dict[str, float]:
            out: dict[str, float] = {}
            for k, v in (d or {}).items():
                try:
                    out[str(k)] = float(v)
                except (ValueError, TypeError):
                    continue
            return out

        if {"by_stage", "by_exec", "exec_time_s"} & set(raw):
            return cls(_names(raw.get("by_stage")), _names(raw.get("by_exec")),
                       _times(raw.get("exec_time_s")))
        # A bare mapping is the older shape from the metrics bundle: stage -> query.
        return cls(_names(raw))

    def resolve(self, exec_id: int, stage_ids: frozenset[int]) -> Optional[str]:
        """The query name for a unit, or ``None`` when the side-car does not cover it."""
        if exec_id in self.by_exec:
            return self.by_exec[exec_id]
        names = {self.by_stage[s] for s in stage_ids if s in self.by_stage}
        if not names:
            return None
        if len(names) > 1:
            self.conflicts.append(
                f"exec {exec_id}: stages name {len(names)} queries ({', '.join(sorted(names))})")
            return None
        return next(iter(names))

    def timing(self, query_id: Optional[str]) -> Optional[float]:
        """The harness's execution time for a named unit, in seconds."""
        return self.exec_time_s.get(query_id) if query_id else None


def _sub_state(state: _State, stage_ids: frozenset[int], exec_ids: frozenset[int],
               start_ts: int, end_ts: int) -> _State:
    """A ``_State`` holding only one unit's stages and SQL-metric attribution.

    Application-wide facts (executor geometry, the first-executor timestamp) are
    carried over unchanged: they describe the cluster the unit ran on, not the unit.
    The scan-metric maps are scoped to the unit's executions so a stage cannot be
    credited with another query's file or partition counts.
    """
    sub = _State()
    sub.app_id = state.app_id
    sub.app_name = state.app_name
    sub.start_ts = start_ts
    sub.end_ts = end_ts
    sub.executor_cores = state.executor_cores
    sub.executor_memory = state.executor_memory
    sub._peak_executors = state._peak_executors
    # Kept application-wide on purpose. For every unit after the first it is earlier
    # than the unit's own start, so resource_wait_s is 0 - which is the truth: the
    # executors were already held.
    sub._first_executor_ts = state._first_executor_ts

    sub.stages = {sid: state.stages[sid] for sid in sorted(stage_ids) if sid in state.stages}
    sub._stage_first_task_ts = {
        sid: ts for sid, ts in state._stage_first_task_ts.items() if sid in stage_ids
    }
    sub._stage_task_span = {
        sid: span for sid, span in state._stage_task_span.items() if sid in stage_ids
    }
    sub.stageid_to_execid = {
        sid: x for sid, x in state.stageid_to_execid.items() if sid in stage_ids
    }
    for x in exec_ids:
        if x in state.acc_kind:
            sub.acc_kind[x] = dict(state.acc_kind[x])
        if x in state.acc_node:
            sub.acc_node[x] = dict(state.acc_node[x])
        if x in state.accum_value:
            sub.accum_value[x] = copy.copy(state.accum_value[x])
        if x in state.accid_to_stageids:
            sub.accid_to_stageids[x] = {
                a: {s for s in ss if s in stage_ids}
                for a, ss in state.accid_to_stageids[x].items()
            }
        if x in state.node_name_to_stageids:
            sub.node_name_to_stageids[x] = {
                n: {s for s in ss if s in stage_ids}
                for n, ss in state.node_name_to_stageids[x].items()
            }
    return sub


def split_state(state: _State, mode: str = SPLIT_NONE,
                sidecar: Optional[Sidecar] = None) -> list[tuple[Unit, _State]]:
    """Cut one parsed application into ``(unit, sub-state)`` pairs, in run order.

    ``none`` returns the state itself under a single application unit, so the caller
    has one code path. ``sql`` returns one pair per SQL execution that produced at
    least one stage; an execution with no stages (the driver's opening statement, a
    DDL served from the metastore) is dropped - it has no job graph to model.
    """
    if mode not in SPLITTERS:
        raise ValueError(f"unknown split mode {mode!r}; expected one of {', '.join(SPLITTERS)}")

    if mode == SPLIT_NONE or not state.app_id:
        return [(Unit(query_id=state.app_id, unit_kind="application", unit_id=state.app_id,
                      start_ts=state.start_ts, end_ts=state.end_ts,
                      stage_ids=frozenset(state.stages)), state)]

    by_exec: dict[int, set[int]] = {}
    for sid in state.stages:
        x = state.stageid_to_execid.get(sid)
        if x is not None:
            by_exec.setdefault(x, set()).add(sid)

    units: list[tuple[Unit, _State]] = []
    for x in sorted(by_exec):
        stage_ids = frozenset(by_exec[x])
        start_ts = state.sql_exec_start.get(x, 0)
        end_ts = state.sql_exec_end.get(x, 0)
        if not (start_ts and end_ts):
            # No SQL span in the log: fall back to the stage span so the unit is still
            # emitted with a real, if narrower, wall-clock.
            subs = [state.stages[s]["submission_ts"] for s in stage_ids if state.stages[s]["submission_ts"]]
            comps = [state.stages[s]["completion_ts"] for s in stage_ids if state.stages[s]["completion_ts"]]
            start_ts = start_ts or (min(subs) if subs else 0)
            end_ts = end_ts or (max(comps) if comps else 0)

        name = sidecar.resolve(x, stage_ids) if sidecar else None
        units.append((
            Unit(query_id=name or f"{state.app_id}#exec{x}",
                 unit_kind="sql_execution", unit_id=str(x),
                 start_ts=start_ts, end_ts=end_ts, stage_ids=stage_ids,
                 labelled=name is not None,
                 harness_exec_time_s=sidecar.timing(name) if sidecar else None),
            _sub_state(state, stage_ids, frozenset({x}), start_ts, end_ts),
        ))
    return units
