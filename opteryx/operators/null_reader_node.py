# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Null Reader Node

This operator is used when a FILTER(FALSE) condition has been detected,
indicating that no rows can possibly match the predicate. Instead of reading
from the underlying connector, we short-circuit and return an empty table
with the correct schema.

This is more efficient than reading all rows and filtering them out.
"""

import logging
from typing import Generator

import pyarrow
from orso.schema import convert_orso_schema_to_arrow_schema

from opteryx import EOS

from . import BasePlanNode

logger = logging.getLogger(__name__)


class NullReaderNode(BasePlanNode):  # pragma: no cover
    """
    Returns an empty table with the correct schema.

    Used when contradictory predicates make the result empty.
    """

    def __init__(self, properties, **parameters):
        """Initialize NullReaderNode."""
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.columns = parameters.get("columns", [])
        self.relations = parameters.get("relations", [])
        self.schema = parameters.get("schema")

    def execute(self, morsel, **_kwargs) -> Generator:
        """Return empty table with correct schema."""
        if morsel == EOS:
            yield None
            return

        # Try to build empty table with correct schema
        # First try: use schema property if available
        if self.schema:
            try:
                arrow_schema = convert_orso_schema_to_arrow_schema(self.schema)
                empty_table = pyarrow.table(
                    {
                        name: pyarrow.array([], type=arrow_schema.field(name).type)
                        for name in arrow_schema.names
                    }
                )
                yield empty_table
                return
            except (
                ValueError,
                TypeError,
                pyarrow.lib.ArrowInvalid,
            ) as err:  # pragma: no cover - defensive fallback
                logger.debug("Unable to build schema-aware empty table: %s", err)

        # Second try: use columns property if available
        if self.columns:
            # Create empty table with column names but no types
            # Extract column names (handle both string and LogicalColumn objects)
            col_names = []
            for col in self.columns:
                if isinstance(col, str):
                    col_names.append(col)
                elif hasattr(col, "name"):
                    col_names.append(col.name)
                else:
                    col_names.append(str(col))

            # Create empty table with column structure
            empty_table = pyarrow.table(
                {col_name: pyarrow.array([], type=pyarrow.null()) for col_name in col_names}
            )
            yield empty_table
            return

        # Fallback: return completely empty table
        empty_table = pyarrow.table({})
        yield empty_table

    @property
    def name(self):  # pragma: no cover
        """Friendly name for this step"""
        return "Null Reader"

    @property
    def config(self):
        """Additional details for this step"""
        return "(empty table - contradictory predicates)"

    def __repr__(self):  # pragma: no cover
        return f"<{self.__class__.__name__}>"
