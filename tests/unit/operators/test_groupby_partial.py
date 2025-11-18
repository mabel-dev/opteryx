# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from types import SimpleNamespace

import pyarrow

from opteryx import EOS
from opteryx.models import QueryProperties
from opteryx.operators.aggregate_and_group_node import AggregateAndGroupNode
from opteryx.operators.base_plan_node import BasePlanNode


def _build_test_node(max_buffer_size=2):
    props = QueryProperties(qid="test", variables={})
    node = AggregateAndGroupNode.__new__(AggregateAndGroupNode)
    BasePlanNode.__init__(node, properties=props, columns=[])

    node.groups = []
    node.aggregates = []
    node.all_identifiers = ["group", "value"]
    node.evaluatable_nodes = []
    node.group_by_columns = ["group"]
    node.column_map = {"value_sum": "value_sum"}
    node.aggregate_functions = [("value", "sum", None)]

    fake_schema_column = SimpleNamespace(identity="value_sum")
    fake_agg_node = SimpleNamespace(schema_column=fake_schema_column)
    node._aggregator_metadata = [(fake_agg_node, ("value", "sum", None))]
    (
        node._partial_merge_aggs,
        node._final_partial_column_map,
        node._buffer_partial_column_map,
    ) = node._build_partial_merge_plan()

    node.buffer = []
    node.max_buffer_size = max_buffer_size
    node._partial_aggregated = False
    node._disable_partial_agg = False

    return node


def _drain(node, morsel):
    for _ in node.execute(morsel):
        pass


def _collect_results(node):
    tables = []
    for result in node.execute(EOS):
        if result is not None and result != EOS:
            tables.append(result)
    return tables


def test_partial_aggregation_consumes_subsequent_rows():
    node = _build_test_node(max_buffer_size=2)

    morsels = [
        pyarrow.Table.from_pydict({"group": ["a"], "value": [1]}),
        pyarrow.Table.from_pydict({"group": ["a"], "value": [2]}),
        pyarrow.Table.from_pydict({"group": ["b"], "value": [3]}),
        pyarrow.Table.from_pydict({"group": ["a"], "value": [4]}),
    ]

    for morsel in morsels:
        _drain(node, morsel)

    tables = _collect_results(node)
    assert len(tables) == 1

    result = tables[0].to_pydict()
    values_by_group = dict(zip(result["group"], result["value_sum"]))
    assert values_by_group == {"a": 7, "b": 3}


def test_partial_buffer_merging_keeps_totals():
    node = _build_test_node(max_buffer_size=2)

    morsels = [
        pyarrow.Table.from_pydict({"group": ["a"], "value": [1]}),
        pyarrow.Table.from_pydict({"group": ["a"], "value": [1]}),
        pyarrow.Table.from_pydict({"group": ["a"], "value": [1]}),
        pyarrow.Table.from_pydict({"group": ["b"], "value": [5]}),
        pyarrow.Table.from_pydict({"group": ["b"], "value": [5]}),
    ]

    for morsel in morsels:
        _drain(node, morsel)

    tables = _collect_results(node)
    assert len(tables) == 1

    result = tables[0].to_pydict()
    values_by_group = dict(zip(result["group"], result["value_sum"]))
    assert values_by_group == {"a": 3, "b": 10}
