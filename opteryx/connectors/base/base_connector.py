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
The BaseConnector provides a common interface for all storage connectors.
"""
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional

import pyarrow
from orso.schema import RelationSchema

from opteryx.models import QueryStatistics

MIN_CHUNK_SIZE: int = 500
INITIAL_CHUNK_SIZE: int = 500
DEFAULT_MORSEL_SIZE: int = 16 * 1024 * 1024


class BaseConnector:
    @property
    def __mode__(self):  # pragma: no cover
        raise NotImplementedError("__mode__ not defined")

    @property
    def interal_only(self):
        return False

    def __init__(
        self,
        *,
        dataset: str = None,
        config: Dict[str, Any] = None,
        statistics: QueryStatistics,
        **kwargs,
    ) -> None:
        """
        Initialize the base connector with configuration.

        Args:
            dataset: The name of the dataset to read.
            config: Configuration information specific to the connector.
        """
        if config is None:
            self.config = {}
        else:
            self.config = config.copy()
        self.dataset = dataset
        self.chunk_size = INITIAL_CHUNK_SIZE
        self.schema = None
        self.statistics = statistics
        self.pushed_predicates: list = []

    def get_dataset_schema(self) -> RelationSchema:  # pragma: no cover
        """
        Retrieve the schema of a dataset.

        Returns:
            A RelationSchema representing the schema of the dataset.
        """
        raise NotImplementedError("Subclasses must implement get_dataset_schema method.")

    def read_dataset(self, **kwargs) -> Iterable:  # pragma: no cover
        """
        Read a dataset and return a reader object.

        Args:
            dataset_name: Name of the dataset.

        Returns:
            A reader object for iterating over the dataset.
        """
        raise NotImplementedError("Subclasses must implement read_dataset method.")

    def read_schema_from_metastore(self):
        # to be implemented
        return None

    def chunk_dictset(
        self,
        dictset: Iterable[dict],
        columns: Optional[list] = None,
        morsel_size: int = DEFAULT_MORSEL_SIZE,
        initial_chunk_size: int = INITIAL_CHUNK_SIZE,
    ) -> pyarrow.Table:
        chunk = []
        self.chunk_size = initial_chunk_size  # we reset each time
        morsel = None

        for index, record in enumerate(dictset):
            _id = record.pop("_id", None)
            # column selection
            if columns:
                record = {k.name: record.get(k.name) for k in columns}
            record["id"] = None if _id is None else str(_id)

            chunk.append(record)

            if index == self.chunk_size - 1:
                morsel = pyarrow.Table.from_pylist(chunk)
                # Estimate the number of records to fill the morsel size
                if morsel.nbytes > 0:
                    self.chunk_size = int(morsel_size // (morsel.nbytes / self.chunk_size))
                yield morsel
                chunk = []
            elif (index > self.chunk_size - 1) and (index - self.chunk_size) % self.chunk_size == 0:
                morsel = pyarrow.Table.from_pylist(chunk)
                yield morsel
                chunk = []

        if chunk:
            morsel = pyarrow.Table.from_pylist(chunk)
            yield morsel


class DatasetReader:
    def __init__(self, dataset_name: str, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize the reader with configuration.

        Args:
            config: Configuration information specific to the reader.
        """
        self.dataset_name = dataset_name
        self.config = config

    def __iter__(self) -> "DatasetReader":
        """
        Make the reader object iterable.
        """
        return self

    def __next__(self) -> pyarrow.Table:  # pragma: no cover
        """
        Read the next chunk or morsel from the dataset.

        Returns:
            A pyarrow Table representing a chunk or morsel of the dataset.
            raises StopIteration if the dataset is exhausted.
        """
        raise NotImplementedError("Subclasses must implement __next__ method.")

    def close(self) -> None:
        """
        Close the reader and release any resources.
        """
        pass
