# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Blob Reader Node

This is a SQL Query Execution Plan Node.

This Node creates datasets based on function calls like VALUES and UNNEST.
"""

import time
from typing import Generator

import pyarrow

from opteryx.exceptions import SqlError
from opteryx.managers.expression import NodeType
from opteryx.models import QueryProperties
from opteryx.utils import series

from .read_node import ReaderNode


def _generate_series(**kwargs):
    value_array = series.generate_series(*kwargs["args"])
    column_name = kwargs["columns"][0].schema_column.identity
    return pyarrow.Table.from_arrays([value_array], [column_name])


def _unnest(**kwargs):
    """unnest converts an list into rows"""
    if kwargs["args"][0].node_type == NodeType.NESTED:
        list_items = [kwargs["args"][0].centre.value]
    else:
        list_items = kwargs["args"][0].value
    column_name = kwargs["columns"][0].schema_column.identity

    return pyarrow.Table.from_arrays([list_items], [column_name])


def _values(**parameters):
    columns = [col.schema_column.identity for col in parameters["columns"]]
    values_array = parameters["values"]
    return [{columns[i]: value.value for i, value in enumerate(values)} for values in values_array]


def _fake_data(**kwargs):
    from orso.faker import generate_fake_data

    rows = kwargs["rows"]
    schema = kwargs["schema"]
    for column in schema.columns:
        column.name = column.identity
    return generate_fake_data(schema, rows)


def _http(**kwargs):
    aliases = kwargs.get("schema")
    data = kwargs.get("data")

    renames = [aliases.column(column).identity for column in data.column_names]
    data = data.rename_columns(renames)

    return data


DATASET_FUNCTIONS = {
    "FAKE": _fake_data,
    "GENERATE_SERIES": _generate_series,
    "UNNEST": _unnest,
    "VALUES": _values,
    "HTTP": _http,
}


class FunctionDatasetNode(ReaderNode):
    def __init__(self, properties: QueryProperties, **parameters):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        ReaderNode.__init__(self, properties=properties, **parameters)
        self.alias = parameters.get("alias")
        self.function = parameters["function"]
        self.parameters = parameters
        self.columns = parameters.get("columns", [])
        self.args = parameters.get("args", [])

    @property
    def config(self):  # pragma: no cover
        from opteryx.managers.expression import format_expression

        if self.function == "FAKE":
            return f"FAKE ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.alias if self.alias else ''})"
        if self.function == "GENERATE_SERIES":
            return f"GENERATE SERIES ({', '.join(format_expression(arg) for arg in self.args)}){' AS ' + self.alias if self.alias else ''}"
        if self.function == "VALUES":
            return f"VALUES (({', '.join(self.columns)}) x {len(self.values)} AS {self.alias})"
        if self.function == "UNNEST":
            return f"UNNEST ({', '.join(format_expression(arg) for arg in self.args)}{' AS ' + self.parameters.get('unnest_target', '')})"
        if self.function == "HTTP":
            return f"HTTP ({self.url}) AS {self.alias}"

    @property
    def name(self):  # pragma: no cover
        return "Dataset Constructor"

    @property
    def can_push_selection(self):
        return False

    def execute(self, morsel, **kwargs) -> Generator:
        try:
            start_time = time.time_ns()
            data = DATASET_FUNCTIONS[self.function](**self.parameters)  # type:ignore
            self.statistics.time_evaluate_dataset += time.time_ns() - start_time
        except TypeError as err:  # pragma: no cover
            if str(err).startswith("_unnest() takes 2"):
                raise SqlError(
                    "UNNEST expects a literal list in paranthesis, or a field name as a parameter."
                )
            raise err

        if isinstance(data, list):
            table = pyarrow.Table.from_pylist(data)
        elif hasattr(data, "arrow"):
            table = data.arrow()
        else:
            table = data

        self.statistics.columns_read += len(table.column_names)
        self.statistics.rows_read += table.num_rows
        self.statistics.bytes_processed += table.nbytes

        yield table
