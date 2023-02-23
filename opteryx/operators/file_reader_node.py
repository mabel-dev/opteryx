# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
File Reader Node

This is a SQL Query Execution Plan Node.

This Node reads files to a PyArrow Table.
"""
import time
from typing import Iterable

import pyarrow

from opteryx.managers.expression import ExpressionTreeNode
from opteryx.managers.expression import NodeType
from opteryx.models import Columns
from opteryx.models import QueryProperties
from opteryx.operators import BasePlanNode
from opteryx.utils.file_decoders import KNOWN_EXTENSIONS


def _normalize_to_types(table):
    """
    Normalize types e.g. all numbers are decimal128 and dates
    """
    schema = table.schema

    for index, column_name in enumerate(schema.names):
        type_name = str(schema.types[index])
        if type_name in ("date32[day]", "date64", "timestamp[s]", "timestamp[ms]"):
            schema = schema.set(
                index,
                pyarrow.field(
                    name=column_name,
                    type=pyarrow.timestamp("us"),
                    metadata=table.field(column_name).metadata,
                ),
            )
        if type_name == ("list<item: null>"):
            schema = schema.set(
                index,
                pyarrow.field(
                    name=column_name,
                    type=pyarrow.list_(pyarrow.string()),
                    metadata=table.field(column_name).metadata,
                ),
            )

    return table.cast(target_schema=schema), schema


class FileReaderNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(properties=properties)

        self._dataset: str = config.get("dataset", None)
        self._alias: list = config.get("alias", None)

        self._filter = None

        self._reader = config.get("reader")  # type:ignore

        # pushed down projections
        self._selection = config.get("selection")
        if isinstance(self._selection, list):
            self._selection = set(self._selection)

    @property
    def can_push_selection(self):
        return True

    def push_predicate(self, predicate):
        # For the blob reader, we push selection nodes, for parquet we then convert
        # these to DNF at read time, for everything else, we run the selection nodes
        from opteryx.connectors.capabilities import predicate_pushable

        if predicate_pushable.to_dnf(predicate) is None:
            # we can't push all predicates everywhere
            return False
        if self._filter is None:
            self._filter = predicate
            return True
        self._filter = ExpressionTreeNode(NodeType.AND, left=predicate, right=self._filter)
        return True

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        if isinstance(self._dataset, str):
            return f"{self._dataset}"
        return "<complex dataset>"

    @property
    def name(self):  # pragma: no cover
        return "File Reader"

    def execute(self) -> Iterable:
        ext = ".".join(self._dataset.split("/")[-1].split(".")[1:])
        parser, kind = KNOWN_EXTENSIONS[ext]

        time_to_read, blob_bytes, pyarrow_blob = self._read_and_parse(
            (
                self._dataset,
                self._reader.read_blob,
                parser,
                self._selection,
                self._filter,
            )
        )

        # we've read a blob
        self.statistics.count_data_blobs_read += 1

        # extract stats from reader
        self.statistics.bytes_read_data += blob_bytes
        self.statistics.time_data_read += time_to_read

        # we should know the number of entries
        self.statistics.rows_read += pyarrow_blob.num_rows
        self.statistics.bytes_processed_data += pyarrow_blob.nbytes

        pyarrow_blob = Columns.create_table_metadata(
            table=pyarrow_blob,
            expected_rows=0,
            name=self._dataset,
            table_aliases=[self._alias],
            disposition="blob",
            path=self._dataset,
        )
        self.statistics.columns_read += len(pyarrow_blob.column_names)

        pyarrow_blob, schema = _normalize_to_types(pyarrow_blob)

        # break up large files
        batch_size = (96 * 1024 * 1024) // (pyarrow_blob.nbytes / pyarrow_blob.num_rows)
        for batch in pyarrow_blob.to_batches(max_chunksize=batch_size):
            yield pyarrow.Table.from_batches([batch], schema=pyarrow_blob.schema)

    def _read_and_parse(self, config):
        path, reader, parser, projection, selection = config
        start_read = time.time_ns()
        blob_bytes = reader(path)
        table = parser(blob_bytes, projection, selection)
        time_to_read = time.time_ns() - start_read
        return time_to_read, blob_bytes.getbuffer().nbytes, table
