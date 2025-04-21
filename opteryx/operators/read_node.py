# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Read Node

This is the SQL Query Execution Plan Node responsible for the reading of data.

It wraps different internal readers (e.g. GCP Blob reader, SQL Reader),
normalizes the data into the format for internal processing.
"""

import time
from typing import Generator

import orjson
import pyarrow
from orso.schema import RelationSchema
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS
from opteryx.models import QueryProperties

from . import BasePlanNode


def struct_to_jsonb(table: pyarrow.Table) -> pyarrow.Table:
    """
    Converts any STRUCT columns in a PyArrow Table to JSON strings and replaces them
    in the same column position.

    Parameters:
        table (pa.Table): The PyArrow Table to process.

    Returns:
        pa.Table: A new PyArrow Table with STRUCT columns converted to JSON strings.
    """
    for i in range(table.num_columns):
        field = table.schema.field(i)

        # Check if the column is a STRUCT
        if pyarrow.types.is_struct(field.type):
            # Convert each row in the STRUCT column to a JSON string
            json_strings = [
                orjson.dumps(row.as_py()) if row.is_valid else None for row in table.column(i)
            ]
            json_array = pyarrow.array(json_strings, type=pyarrow.binary())

            # Drop the original STRUCT column
            table = table.drop_columns(field.name)

            # Insert the new JSON column at the same position
            table = table.add_column(
                i, pyarrow.field(name=field.name, type=pyarrow.binary()), json_array
            )

        # Check for LIST<STRUCT>
        if pyarrow.types.is_list(field.type) and pyarrow.types.is_struct(field.type.value_type):
            list_array = table.column(i)

            # Convert each list element
            converted_data = []
            for item in list_array:
                if item is None:
                    converted_data.append(None)
                else:
                    # Each item is a list of structs
                    converted_list = []
                    for struct in item.as_py():
                        if struct is None:
                            converted_list.append(None)
                        else:
                            converted_list.append(orjson.dumps(struct))
                    converted_data.append(converted_list)

            # Build the new array
            jsonb_array = pyarrow.array(converted_data, type=pyarrow.list_(pyarrow.binary()))

            # Drop original column and insert new one at same position
            table = table.drop_columns(field.name)
            table = table.add_column(
                i, pyarrow.field(name=field.name, type=jsonb_array.type), jsonb_array
            )

    return table


def normalize_morsel(schema: RelationSchema, morsel: pyarrow.Table) -> pyarrow.Table:
    if morsel.column_names == ["$COUNT(*)"]:
        return morsel
    if len(schema.columns) == 0 and morsel.column_names != ["*"]:
        one_column = pyarrow.array([True] * morsel.num_rows, type=pyarrow.bool_())
        morsel = morsel.append_column("*", one_column)
        return morsel.select(["*"])

    # rename columns for internal use
    target_column_names = []
    # columns in the data but not in the schema, droppable
    droppable_columns = []

    # Find which columns to drop and which columns we already have
    for i, column in enumerate(morsel.column_names):
        column_name = schema.find_column(column)
        if column_name is None:
            droppable_columns.append(i)
        else:
            target_column_names.append(str(column_name))

    # Remove from the end otherwise we'll remove the wrong columns after we've removed one
    droppable_columns.reverse()
    for droppable in droppable_columns:
        morsel = morsel.remove_column(droppable)

    # remane columns to the internal names (identities)
    morsel = morsel.rename_columns(target_column_names)

    # add columns we don't have, populate with nulls but try to get the correct type
    for column in schema.columns:
        if column.identity not in target_column_names:
            null_column = pyarrow.array([None] * morsel.num_rows, type=column.arrow_field.type)
            field = pyarrow.field(name=column.identity, type=column.arrow_field.type)
            morsel = morsel.append_column(field, null_column)

    # ensure the columns are in the right order
    return morsel.select([col.identity for col in schema.columns])


def merge_schemas(
    hypothetical_schema: RelationSchema, observed_schema: pyarrow.Schema
) -> pyarrow.schema:
    """
    Using the hypothetical schema as the base, replace with fields from the observed schema
    which are a Decimal type.
    """
    # convert the Orso schema to an Arrow schema
    hypothetical_arrow_schema = convert_orso_schema_to_arrow_schema(hypothetical_schema, True)

    # Convert the hypothetical schema to a dictionary for easy modification
    schema_dict = {field.name: field for field in hypothetical_arrow_schema}

    # Iterate through fields in the observed schema
    for observed_field in observed_schema:
        # Check if the field is of type Decimal or List/Array
        if pyarrow.types.is_decimal(observed_field.type) or pyarrow.types.is_list(
            observed_field.type
        ):
            # Replace or add the field to the schema dictionary
            schema_dict[observed_field.name] = observed_field

    # Create a new schema from the updated dictionary of fields
    merged_schema = pyarrow.schema(list(schema_dict.values()))

    return merged_schema


class ReaderNode(BasePlanNode):
    is_scan = True

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)

        self.uuid = parameters.get("uuid")
        self.start_date = parameters.get("start_date")
        self.end_date = parameters.get("end_date")
        self.hints = parameters.get("hints", [])
        self.columns = parameters.get("columns", [])
        self.predicates = parameters.get("predicates", [])

        self.connector = parameters.get("connector")
        self.schema = parameters.get("schema")
        self.limit = parameters.get("limit")

        if len(self.hints) != 0:
            self.statistics.add_message("All HINTS are currently ignored")

        self.statistics.rows_read += 0
        self.statistics.columns_read += 0

    def to_mermaid(self, stats, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        BAR = "------------------------<br />"

        if self.connector is None:
            mermaid = f'NODE_{nid}[("**{self.node_type.upper()} (FUNCTION)**<br />'
            mermaid += f"{self.function}<br />"
        else:
            mermaid = f'NODE_{nid}[(**"{self.node_type.upper()} ({self.connector.__type__})**<br />'
            mermaid += f"{self.connector.dataset}<br />"
        mermaid += BAR
        if self.columns:
            mermaid += f"columns: {len(self.columns)}<br />" + BAR
        if self.predicates:
            mermaid += "filters<br />" + BAR
        if self.limit:
            mermaid += f"limit: {self.limit:,}<br />" + BAR
        if self.start_date:
            mermaid += f"start date: {self.start_date}<br />"
            mermaid += f"end date: {self.end_date}<br />"
            mermaid += BAR

        if hasattr(self, "rows_seen") or hasattr(self, "blobs_seen"):
            if hasattr(self, "blobs_seen"):
                mermaid += f"reads: {self.blobs_seen:,}<br />"
            if hasattr(self, "rows_seen"):
                mermaid += f"rows seen: {self.rows_seen:,}<br />"
            mermaid += BAR

        elif hasattr(self.connector, "rows_seen") or hasattr(self.connector, "blobs_seen"):
            if hasattr(self.connector, "blobs_seen"):
                mermaid += f"reads: {self.connector.blobs_seen:,}<br />"
            if hasattr(self.connector, "rows_seen"):
                mermaid += f"rows seen: {self.connector.rows_seen:,}<br />"
            mermaid += BAR

        mermaid += f"({stats.get('time_ms', 0):,.2f}ms)"
        return mermaid + '")]'

    @property
    def name(self):  # pragma: no cover
        """friendly name for this step"""
        return "Read"

    @property
    def config(self):
        """Additional details for this step"""
        date_range = ""
        if self.parameters.get("start_date") == self.parameters.get("end_date"):
            if self.parameters.get("start_date") is not None:
                date_range = f" FOR '{self.parameters.get('start_date')}'"
        else:
            date_range = (
                f" FOR '{self.parameters.get('start_date')}' TO '{self.parameters.get('end_date')}'"
            )
        return (
            f"{self.connector.__type__} "
            f"({self.parameters.get('relation')}"
            f"{' AS ' + self.parameters.get('alias') if self.parameters.get('alias') else ''}"
            f"{date_range}"
            f"{' WITH(' + ','.join(self.parameters.get('hints')) + ')' if self.parameters.get('hints') else ''})"
        )

    def execute(self, morsel, **kwargs) -> Generator:
        """Perform this step, time how long is spent doing work"""
        if morsel == EOS:
            yield None
            return

        morsel = None
        orso_schema = self.schema
        orso_schema_cols = []
        for col in orso_schema.columns:
            if col.identity in [c.schema_column.identity for c in self.columns]:
                orso_schema_cols.append(col)
        orso_schema.columns = orso_schema_cols
        arrow_schema = None
        start_clock = time.monotonic_ns()
        reader = self.connector.read_dataset(
            columns=self.columns, predicates=self.predicates, limit=self.limit
        )
        for morsel in reader:
            # try to make each morsel have the same schema
            morsel = struct_to_jsonb(morsel)
            morsel = normalize_morsel(orso_schema, morsel)
            if arrow_schema is None:
                arrow_schema = merge_schemas(self.schema, morsel.schema)
            if arrow_schema.names:
                morsel = morsel.cast(arrow_schema)

            self.statistics.time_reading_blobs += time.monotonic_ns() - start_clock
            self.statistics.blobs_read += 1
            self.statistics.rows_read += morsel.num_rows
            self.statistics.bytes_processed += morsel.nbytes
            yield morsel
            start_clock = time.monotonic_ns()
        if morsel:
            self.statistics.columns_read += morsel.num_columns
        else:
            self.statistics.columns_read += len(orso_schema.columns)
