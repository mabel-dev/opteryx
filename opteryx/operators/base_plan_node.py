# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import time
from typing import Optional
from typing import Union

import pyarrow
from orso.tools import random_string
from pyarrow import Table

from opteryx import EOS
from opteryx.draken import Morsel

END = object()


class BasePlanNode:
    is_join: bool = False
    is_scan: bool = False
    is_not_explained: bool = False
    is_stateless: bool = False

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
        self.columns = parameters.get("columns", [])

        self._time_stat_key = f"time_{self.name.lower().replace(' ', '_')}"
        self._empty_morsel_cache = None

    @property
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

    def to_mermaid(self, stats, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        BAR = "------------------------<br />"

        mermaid = f'NODE_{nid}["**{self.node_type.upper()}**<br />'
        if stats is None:
            reportable_stats = {}
        else:
            reportable_stats = {k: v for k, v in stats.items() if k in ["calls", "time_ms"]}
            if reportable_stats:
                mermaid += BAR
                if self.columns:
                    mermaid += f"columns: {len(self.columns)}<br />" + BAR
                if hasattr(self, "limit") and self.limit is not None:
                    mermaid += f"limit: {self.limit:,}<br />" + BAR
                if hasattr(self, "order") and self.limit is not None:
                    mermaid += f"order<br />" + BAR
                mermaid += f"calls: {reportable_stats.get('calls'):,}<br />"
                mermaid += BAR
                mermaid += f"({reportable_stats.get('time_ms', 0):,.2f}ms)"
        return mermaid + '"]'

    def __str__(self) -> str:
        return f"{self.name} {self.sensors()}"

    def execute(self, morsel: pyarrow.Table) -> Optional[pyarrow.Table]:  # pragma: no cover
        raise NotImplementedError()

    def ensure_arrow_table(self, morsel: Union[Table, Morsel]) -> Table:
        """Ensure the provided morsel is a PyArrow table when needed."""
        if morsel is EOS:
            return EOS
        if isinstance(morsel, Morsel):
            self.statistics.morsel_to_table_conversion += 1
            return morsel.to_arrow()
        return morsel

    def ensure_draken_morsel(self, table: Union[Table, Morsel]):
        """Ensure the provided morsel is a Draken morsel when needed.

        Returns either a single Morsel or a generator of Morsels.
        """
        if table is EOS:
            return EOS
        if isinstance(table, Table):
            self.statistics.table_to_morsel_conversion += 1
            # Use iter_from_arrow to avoid expensive combine_chunks
            # Yields morsels aligned with Arrow chunk boundaries
            return Morsel.iter_from_arrow(table)
        return table

    def __call__(self, morsel: pyarrow.Table, join_leg: str) -> Optional[pyarrow.Table]:
        # Cache frequently accessed attributes
        statistics = self.statistics
        time_stat_key = self._time_stat_key
        is_scan = self.is_scan

        # Process input metrics
        if hasattr(morsel, "num_rows"):
            num_rows = morsel.num_rows
            nbytes = morsel.nbytes
            self.records_in += num_rows
            self.bytes_in += nbytes
            self.calls += 1

        # Set up execution
        generator = self.execute(morsel, join_leg=join_leg)
        empty_morsel = None
        at_least_one = False

        while True:
            try:
                start_time = time.monotonic_ns()
                result = next(generator, END)
                execution_time = time.monotonic_ns() - start_time

                self.execution_time += execution_time
                statistics.increase(time_stat_key, execution_time)

                if result == END:
                    if not at_least_one and empty_morsel is not None:
                        yield empty_morsel
                    break

                if is_scan:
                    self.calls += 1

                # Optimized attribute checking
                try:
                    result_num_rows = result.num_rows
                    result_nbytes = result.nbytes
                    self.records_out += result_num_rows
                    self.bytes_out += result_nbytes

                    if empty_morsel is None:
                        empty_morsel = result.slice(0, 0)

                    if result_num_rows > 0:
                        at_least_one = True
                        yield result
                        continue
                    else:
                        statistics.dead_ended_empty_morsels += 1
                except AttributeError:
                    # Not a table-like object
                    pass

                at_least_one = True
                yield result

            except Exception as err:
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

    def to_mermaid(self, stats, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        BAR = "------------------------<br />"

        mermaid = f'NODE_{nid}["**JOIN ({self.join_type.upper()})**<br />'
        if stats is None:
            reportable_stats = {}
        else:
            reportable_stats = {k: v for k, v in stats.items() if k in ["calls", "time_ms"]}
            if reportable_stats:
                mermaid += BAR
                if hasattr(self, "left_filter") and self.left_filter is not None:
                    mermaid += f"bloom filter<br />" + BAR
                mermaid += f"calls: {reportable_stats.get('calls'):,}<br />"
                mermaid += BAR
                mermaid += f"({reportable_stats.get('time_ms', 0):,.2f}ms)"
        return mermaid + '"]'
