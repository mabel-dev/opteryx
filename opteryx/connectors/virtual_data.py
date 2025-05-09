# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
The 'sample' connector provides readers for the internal sample datasets,
$planets, $astronauts, and $satellites.

- $no_table is used in queries where there is no relation specified 'SELECT 1'
- $derived is used as a schema to align virtual columns to
"""

import datetime
import typing

import pyarrow
from orso.schema import RelationSchema

from opteryx import virtual_datasets
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import DatasetReader
from opteryx.connectors.capabilities import Partitionable
from opteryx.connectors.capabilities import Statistics
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils import arrow

WELL_KNOWN_DATASETS = {
    "$astronauts": (virtual_datasets.astronauts, True),
    "$planets": (virtual_datasets.planets, True),
    "$missions": (virtual_datasets.missions, True),
    "$satellites": (virtual_datasets.satellites, True),
    "$variables": (virtual_datasets.variables, True),
    "$derived": (virtual_datasets.derived, False),
    "$no_table": (virtual_datasets.no_table, False),
    "$statistics": (virtual_datasets.statistics, True),
    "$stop_words": (virtual_datasets.stop_words, True),
    "$user": (virtual_datasets.user, True),
}


def suggest(dataset):
    """
    Provide suggestions to the user if they gave a table that doesn't exist.
    """
    from opteryx.utils import suggest_alternative

    known_datasets = (name for name, suggestable in WELL_KNOWN_DATASETS.items() if suggestable)
    suggestion = suggest_alternative(dataset, known_datasets)
    if suggestion is not None:
        return (
            f"The requested dataset, '{dataset}', could not be found. Did you mean '{suggestion}'?"
        )


class SampleDataConnector(BaseConnector, Partitionable, Statistics):
    __mode__ = "Internal"
    __type__ = "SAMPLE"

    def __init__(self, *args, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        Partitionable.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)
        self.dataset = self.dataset.lower()
        self.variables = None

    @property
    def interal_only(self):
        return True

    def read_dataset(self, columns: list = None, **kwargs) -> "DatasetReader":
        return SampleDatasetReader(
            self.dataset,
            columns=columns,
            config=self.config,
            date=self.end_date,
            variables=self.variables,
        )

    def get_dataset_schema(self) -> RelationSchema:
        if self.dataset not in WELL_KNOWN_DATASETS:
            suggestion = suggest(self.dataset)
            raise DatasetNotFoundError(suggestion=suggestion, dataset=self.dataset)
        data_provider, _ = WELL_KNOWN_DATASETS.get(self.dataset)
        self.relation_statistics = data_provider.statistics()
        return data_provider.schema()


class SampleDatasetReader(DatasetReader):
    def __init__(
        self,
        dataset_name: str,
        columns: list,
        config: typing.Optional[typing.Dict[str, typing.Any]] = None,
        date: typing.Union[datetime.datetime, datetime.date, None] = None,
        variables: typing.Dict = None,
    ) -> None:
        """
        Initialize the reader with configuration.

        Args:
            config: Configuration information specific to the reader.
        """
        super().__init__(dataset_name=dataset_name, config=config)
        self.columns = columns
        self.exhausted = False
        self.date = date
        self.variables = variables

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

        data_provider, _ = WELL_KNOWN_DATASETS.get(self.dataset_name)
        if data_provider is None:
            suggestion = suggest(self.dataset_name.lower())
            raise DatasetNotFoundError(suggestion=suggestion, dataset=self.dataset_name)
        table = data_provider.read(self.date, self.variables)
        return arrow.post_read_projector(table, self.columns)
