# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Exit Node

This is a SQL Query Execution Plan Node.

This does the final preparation before returning results to users.

This does two things that the projection node doesn't do:
    - renames columns from the internal names
    - removes all columns not being returned to the user

This node doesn't do any calculations, it is a pure Projection.
"""

import pyarrow
from orso.types import OrsoTypes
from pyarrow import Table

from opteryx import EOS
from opteryx.datatypes.intervals import to_arrow_interval
from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties

from . import BasePlanNode


class ExitNode(BasePlanNode):
    is_not_explained = True

    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.at_least_one = False

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.alias)

        if len(final_columns) != len(set(final_columns)):  # pragma: no cover
            from collections import Counter

            duplicates = [column for column, count in Counter(final_columns).items() if count > 1]
            matches = {a for a, b in zip(final_names, final_columns) if b in duplicates}
            raise AmbiguousIdentifierError(
                message=f"Query result contains multiple instances of the same column(s) - `{'`, `'.join(matches)}`"
            )

        if len(set(final_names)) != len(final_names):  # we have duplicate names
            final_names = []
            for column in self.columns:
                # if column.schema_column.origin:
                #    final_names.append(f"{column.schema_column.origin[0]}.{column.current_name}")
                # else:
                final_names.append(column.alias)

        self.final_columns = final_columns
        self.final_names = final_names

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"

    def execute(self, morsel: Table, **kwargs) -> Table:
        morsel = self.ensure_arrow_table(morsel)

        # Exit doesn't return EOS
        if morsel == EOS:
            if not self.at_least_one:
                from orso.schema import RelationSchema
                from orso.schema import convert_orso_schema_to_arrow_schema

                orso_schema = RelationSchema(
                    name="Relation", columns=[c.schema_column for c in self.columns]
                )
                arrow_shema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)

                morsel = pyarrow.Table.from_arrays(
                    [pyarrow.array([]) for _ in self.columns],
                    schema=arrow_shema,
                )
                morsel = morsel.select(self.final_columns)
                morsel = morsel.rename_columns(self.final_names)
                yield morsel

            yield EOS
            return

        if morsel.num_rows == 0:
            yield None
            return

        self.at_least_one = True

        if not set(self.final_columns).issubset(morsel.column_names):  # pragma: no cover
            mapping = {
                name: int_name for name, int_name in zip(self.final_columns, self.final_names)
            }
            missing_references = {
                mapping.get(ref): ref
                for ref in self.final_columns
                if ref not in morsel.column_names
            }

            raise InvalidInternalStateError(
                f"The following fields were not in the resultset - {', '.join(missing_references.keys())}"
            )

        morsel = morsel.select(self.final_columns)

        for index, column in enumerate(self.columns):
            column_array = morsel.column(index)
            column_identity = column.schema_column.identity
            column_type = column.schema_column.type

            if column_type == OrsoTypes.INTERVAL:
                converted = to_arrow_interval(column_array)
                morsel = morsel.set_column(index, column_identity, converted)
                continue

            if column_type == OrsoTypes.VARCHAR and (
                pyarrow.types.is_binary(column_array.type)
                or pyarrow.types.is_large_binary(column_array.type)
                or pyarrow.types.is_fixed_size_binary(column_array.type)
            ):
                converted = column_array.cast(pyarrow.string())
                morsel = morsel.set_column(index, column_identity, converted)

        morsel = morsel.rename_columns(self.final_names)

        yield morsel
