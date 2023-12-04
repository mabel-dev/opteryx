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
Arrow Reader

Used to read datasets registered using the register_arrow or register_df functions.
"""

import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema

from opteryx.connectors.base.base_connector import DEFAULT_MORSEL_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.shared import MaterializedDatasets
from opteryx.utils import arrow


class ArrowConnector(BaseConnector):
    __mode__ = "Internal"

    def __init__(self, *args, **kwargs):
        BaseConnector.__init__(self, **kwargs)

        self.dataset = self.dataset.lower()
        self._datasets = MaterializedDatasets()

    def get_dataset_schema(self) -> RelationSchema:
        dataset = self._datasets[self.dataset]
        arrow_schema = dataset.schema

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        return self.schema

    def read_dataset(self, columns: list = None, **kwargs) -> pyarrow.Table:
        dataset = self._datasets[self.dataset]

        batch_size = DEFAULT_MORSEL_SIZE // (dataset.nbytes / dataset.num_rows)

        for batch in dataset.to_batches(max_chunksize=batch_size):
            morsel = pyarrow.Table.from_batches([batch], schema=dataset.schema)
            if columns:
                morsel = arrow.post_read_projector(morsel, columns)
            yield morsel
