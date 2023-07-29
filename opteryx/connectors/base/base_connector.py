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
import typing

import pyarrow
from orso.schema import RelationSchema


class BaseConnector:
    @property
    def __mode__(self):
        raise NotImplementedError("__mode__ not defined")

    @property
    def interal_only(self):
        return False

    def __init__(self, *, dataset: str = None, config: typing.Dict[str, typing.Any] = None, **kwargs) -> None:
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

    def get_dataset_schema(self) -> RelationSchema:
        """
        Retrieve the schema of a dataset.

        Returns:
            A RelationSchema representing the schema of the dataset.
        """
        raise NotImplementedError("Subclasses must implement get_dataset_schema method.")

    def read_dataset(self) -> "DatasetReader":
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


class DatasetReader:
    def __init__(
        self, dataset_name: str, config: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> None:
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

    def __next__(self) -> pyarrow.Table:
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
