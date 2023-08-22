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


import pyarrow
from orso.schema import RelationSchema

from opteryx.connectors.base.base_connector import BaseConnector


class HadroConnector(BaseConnector):
    __mode__ = "Collection"

    def __init__(self, **kwargs):
        BaseConnector.__init__(self, **kwargs)

    def read_dataset(self, start_date=None, end_date=None) -> "DatasetReader":
        from hadrodb import HadroDB

        morsel_size = 10000
        if morsel_size is None:
            morsel_size = 100000
        chunk_size = 500

        hadro = HadroDB(collection=self.dataset)
        reader = self.chunk_dictset(hadro.scan(), chunk_size, morsel_size)

        batch = next(reader, None)
        while batch:
            arrays = [pyarrow.array(column) for column in zip(*batch)]
            morsel = pyarrow.Table.from_arrays(arrays, self.schema.column_names)
            # from 500 records, estimate the number of records to fill the morsel size
            if chunk_size == 500 and morsel.nbytes > 0:
                chunk_size = int(morsel_size // (morsel.nbytes / 500))
            yield morsel
            batch = next(reader, None)

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema:
            return self.schema

        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
        if self.schema:
            return self.schema
