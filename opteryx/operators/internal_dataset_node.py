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
Blob Reader Node

This is a SQL Query Execution Plan Node.

This Node reads and parses the data from one of the sample datasets.
"""
import datetime

from typing import Iterable

import pyarrow

from opteryx import samples
from opteryx.exceptions import DatasetNotFoundError
from opteryx.models import Columns, QueryProperties
from opteryx.operators import BasePlanNode


def _normalize_to_types(table):
    """
    Normalize types e.g. all numbers are float64 and dates
    """
    schema = table.schema

    for index, column_name in enumerate(schema.names):
        type_name = str(schema.types[index])
        if type_name in ("date32[day]", "date64", "timestamp[s]", "timestamp[ms]"):
            schema = schema.set(
                index, pyarrow.field(column_name, pyarrow.timestamp("us"))
            )

    return table.cast(target_schema=schema)


def _get_sample_dataset(dataset, alias, end_date):
    # we do this like this so the datasets are not loaded into memory unless
    # they are going to be used
    sample_datasets = {
        "$satellites": samples.satellites,
        "$planets": samples.planets,
        "$astronauts": samples.astronauts,
        "$no_table": samples.no_table,
    }
    dataset = dataset.lower()
    if dataset in sample_datasets:
        table = sample_datasets[dataset](end_date)
        table = _normalize_to_types(table)
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=table.num_rows,
            name=dataset,
            table_aliases=[alias],
        )
        return table
    raise DatasetNotFoundError(f"Dataset not found `{dataset}`.")


class InternalDatasetNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(properties=properties)

        self._alias = config["alias"]
        self._dataset = config["dataset"]

        today = datetime.datetime.utcnow().date()
        self._start_date = config.get("start_date", today)
        self._end_date = config.get("end_date", today)

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        return f"{self._dataset}"

    @property
    def name(self):  # pragma: no cover
        return "Sample Dataset Reader"

    def execute(self) -> Iterable:

        pyarrow_page = _get_sample_dataset(self._dataset, self._alias, self._end_date)
        self.statistics.rows_read += pyarrow_page.num_rows
        self.statistics.bytes_processed_data += pyarrow_page.nbytes
        self.statistics.columns_read += len(pyarrow_page.column_names)

        schema = pyarrow_page.schema

        for index, column_name in enumerate(schema.names):
            type_name = str(schema.types[index])
            if type_name in ("date32[day]", "date64", "timestamp[s]", "timestamp[ms]"):
                schema = schema.set(
                    index,
                    pyarrow.field(
                        name=column_name,
                        type=pyarrow.timestamp("us"),
                        metadata=pyarrow_page.field(column_name).metadata,
                    ),
                )
        pyarrow_page = pyarrow_page.cast(target_schema=schema)

        yield pyarrow_page
