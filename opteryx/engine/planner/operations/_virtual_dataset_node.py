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
from typing import Iterable, Optional

from opteryx import samples
from opteryx.engine.query_statistics import QueryStatistics
from opteryx.engine.planner.operations import BasePlanNode
from opteryx.exceptions import DatabaseError
from opteryx.utils.columns import Columns


def _get_sample_dataset(dataset, alias):
    # we do this like this so the datasets are not loaded into memory unless
    # they are going to be used
    sample_datasets = {
        "$satellites/": samples.satellites,
        "$planets/": samples.planets,
        "$astronauts/": samples.astronauts,
        "$no_table/": samples.no_table,
    }
    dataset = dataset.lower()
    if dataset in sample_datasets:
        table = sample_datasets[dataset]()
        table = Columns.create_table_metadata(
            table=table,
            expected_rows=table.num_rows,
            name=dataset[:-1],
            table_aliases=[alias],
        )
        return table
    raise DatabaseError(f"Dataset not found `{dataset}`.")


class VirtualDatesetNode(BasePlanNode):
    def __init__(self, statistics: QueryStatistics, **config):
        """
        The Blob Reader Node is responsible for reading the relevant blobs
        and returning a Table/Relation.
        """
        super().__init__(statistics=statistics, **config)

        self._statistics = statistics
        self._alias, self._dataset = config.get("dataset", [None, None])

    @property
    def config(self):  # pragma: no cover
        if self._alias:
            return f"{self._dataset} => {self._alias}"
        return f"{self._dataset}"

    @property
    def name(self):  # pragma: no cover
        return "Sample Dataset Reader"

    def execute(self, data_pages: Optional[Iterable]) -> Iterable:
        yield _get_sample_dataset(self._dataset, self._alias)
        return
