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
The 'sample' connector provides readers for the internal sample datasets,
$planets, $astronauts, and $satellites.

- $no_table is used in queries where there is no relation specified 'SELECT 1'
- $derived is used as a schema to align virtual columns to
"""

import typing

import pyarrow
from orso.schema import RelationSchema

from opteryx import samples
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import DatasetReader
from opteryx.exceptions import DatasetNotFoundError

WELL_KNOWN_DATASETS = {
    "$astronauts": samples.astronauts,
    "$derived": None,
    "$no_table": samples.no_table,
    "$planets": samples.planets,
    "$satellites": samples.satellites,
}


def suggest(dataset):
    """
    Provide suggestions to the user if they gave a table that doesn't exist.
    """
    from opteryx.utils import fuzzy_search

    known_datasets = (k for k in WELL_KNOWN_DATASETS if k not in ("$no_table", "$derived"))
    suggestion = fuzzy_search(dataset, known_datasets)
    if suggestion is not None:
        return (
            f"The requested dataset, '{dataset}', could not be found. Did you mean '{suggestion}'?"
        )


class SampleDataConnector(BaseConnector):
    __mode__ = "Internal"

    def read_dataset(self, dataset_name: str) -> "DatasetReader":
        return SampleDatasetReader(dataset_name.lower(), config=self.config)

    def get_dataset_schema(self, dataset_name: str) -> RelationSchema:
        data_provider = WELL_KNOWN_DATASETS.get(dataset_name.lower())
        if data_provider is None:
            suggestion = suggest(dataset_name.lower())
            raise DatasetNotFoundError(message=suggestion, dataset=dataset_name)
        return data_provider.schema


class SampleDatasetReader(DatasetReader):
    def __init__(
        self, dataset_name: str, config: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> None:
        """
        Initialize the reader with configuration.

        Args:
            config: Configuration information specific to the reader.
        """
        super().__init__(dataset_name=dataset_name, config=config)
        self.exhausted = False

    def __next__(self) -> pyarrow.Table:
        """
        Read the next chunk or morsel from the dataset.

        Returns:
            A pyarrow Table representing a chunk or morsel of the dataset.
            raises StopIteration if the dataset is exhausted.
        """
        if self.exhausted:
            raise StopIteration("Dataset has been read.")

        self.exhausted = True

        data_provider = WELL_KNOWN_DATASETS.get(self.dataset_name)
        if data_provider is None:
            suggestion = suggest(self.dataset_name.lower())
            raise DatasetNotFoundError(message=suggestion, dataset=self.dataset_name)
        return data_provider.read()
