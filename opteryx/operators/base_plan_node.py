# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import time
from typing import Optional

import pyarrow
from orso.tools import random_string

from opteryx import EOS

END = object()


class BasePlanNode:
    is_join: bool = False
    is_scan: bool = False

    def __init__(self, *, properties, **parameters):
        """
        This is the base class for nodes in the execution plan.

        The initializer accepts a QueryStatistics node which is populated by different nodes
        differently to record what happened during the query execution.
        """
        from opteryx.models import QueryProperties
        from opteryx.models import QueryStatistics

        self.properties: QueryProperties = properties
        self.statistics: QueryStatistics = QueryStatistics(properties.qid)
        self.parameters = parameters
        self.execution_time = 0
        self.identity = random_string()
        self.calls = 0
        self.records_in = 0
        self.bytes_in = 0
        self.records_out = 0
        self.bytes_out = 0

    @classmethod
    def from_json(cls, json_obj: str) -> "BasePlanNode":  # pragma: no cover
        raise NotImplementedError()

    def config(self) -> str:
        return ""

    @property
    def name(self):  # pragma: no cover
        """
        Friendly Name of this node
        """
        return "no name"

    @property
    def node_type(self) -> str:
        return self.name

    def __str__(self) -> str:
        return f"{self.name} {self.sensors()}"

    def execute(self, morsel: pyarrow.Table) -> Optional[pyarrow.Table]:  # pragma: no cover
        pass

    def __call__(self, morsel: pyarrow.Table, join_leg: str) -> Optional[pyarrow.Table]:
        if hasattr(morsel, "num_rows"):
            self.records_in += morsel.num_rows
            self.bytes_in += morsel.nbytes
            self.calls += 1

        # set up the execution of the operator
        generator = self.execute(morsel, join_leg=join_leg)
        empty_morsel = None
        at_least_one = False

        while True:
            try:
                # Time the production of the next result
                start_time = time.monotonic_ns()
                result = next(generator, END)  # Retrieve the next item from the generator
                execution_time = time.monotonic_ns() - start_time
                self.execution_time += execution_time
                self.statistics.increase("time_" + self.name.lower(), execution_time)

                # Update metrics for valid results
                if result == END:
                    # Break the loop when the generator is exhausted
                    if not at_least_one:
                        yield empty_morsel
                    break

                if hasattr(result, "num_rows"):
                    self.records_out += result.num_rows
                    self.bytes_out += result.nbytes

                    if empty_morsel is None:
                        empty_morsel = result.slice(0, 0)

                    # if we get empty sets, don't yield them unless they're the only one
                    if result.num_rows > 0:
                        self.statistics.avoided_empty_datasets += 1
                        at_least_one = True
                        yield result
                        continue

                yield result

            except Exception as err:
                # print(f"Exception {err} in operator", self.name)
                raise err

    def sensors(self):
        return {
            "calls": self.calls,
            "execution_time": self.execution_time,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }


class JoinNode(BasePlanNode):
    is_join = True

    def __init__(self, *, properties, **parameters):
        super().__init__(properties=properties, **parameters)

        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
