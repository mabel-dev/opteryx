# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Inner (Nested Loop) Join Node

This is a SQL Query Execution Plan Node.

This is an implementation of a nested loop join, which is a simple join algorithm, it excels
when one of the relations is very small - in this situation it's many times faster than a hash
join as we don't need to create the hash table.

The Join Order Optimization Strategy will decide if this node should be used, based on the size.

This is a toy implementation, whilst it is used in production payloads we're playing with
milliseconds of performance difference between this and a hash join.
"""

from threading import Lock

import numpy
import pyarrow
from pyarrow import Table

from opteryx import EOS
from opteryx.compiled.joins.inner_join import nested_loop_join
from opteryx.models import QueryProperties
from opteryx.utils.arrow import align_tables

from . import JoinNode


class NestedLoopJoinNode(JoinNode):
    join_type = "nested_loop"

    def __init__(self, properties: QueryProperties, **parameters):
        JoinNode.__init__(self, properties=properties, **parameters)

        self.left_columns = numpy.array(parameters.get("left_columns"), dtype=numpy.bytes_)
        self.right_columns = numpy.array(parameters.get("right_columns"), dtype=numpy.bytes_)

        self.left_relation = None
        self.left_buffer = []
        self.lock = Lock()

    @property
    def name(self):  # pragma: no cover
        return "Nested Loop Join"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel: Table, join_leg: str) -> Table:
        with self.lock:
            if join_leg == "left":
                if morsel == EOS:
                    self.left_relation = pyarrow.concat_tables(
                        self.left_buffer, promote_options="none"
                    )
                    self.left_buffer.clear()
                else:
                    self.left_buffer.append(morsel)
                yield None
                return

            if join_leg == "right":
                if morsel == EOS:
                    yield EOS
                    return

                left_indexes, right_indexes = nested_loop_join(
                    self.left_relation, morsel, self.left_columns, self.right_columns
                )
                yield align_tables(self.left_relation, morsel, left_indexes, right_indexes)
