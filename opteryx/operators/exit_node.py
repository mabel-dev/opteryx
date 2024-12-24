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

from pyarrow import Table

from opteryx import EOS
from opteryx.exceptions import AmbiguousIdentifierError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryProperties

from . import BasePlanNode


class ExitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    @property
    def config(self):  # pragma: no cover
        return None

    @property
    def name(self):  # pragma: no cover
        return "Exit"

    def execute(self, morsel: Table, **kwargs) -> Table:
        # Exit doesn't return EOS
        if morsel == EOS:
            yield EOS
            return

        final_columns = []
        final_names = []
        for column in self.columns:
            final_columns.append(column.schema_column.identity)
            final_names.append(column.current_name)

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
                final_names.append(column.qualified_name)

        if not set(final_columns).issubset(morsel.column_names):  # pragma: no cover
            mapping = {name: int_name for name, int_name in zip(final_columns, final_names)}
            missing_references = {
                mapping.get(ref): ref for ref in final_columns if ref not in morsel.column_names
            }

            raise InvalidInternalStateError(
                f"The following fields were not in the resultset - {', '.join(missing_references.keys())}"
            )

        morsel = morsel.select(final_columns)
        morsel = morsel.rename_columns(final_names)

        yield morsel
