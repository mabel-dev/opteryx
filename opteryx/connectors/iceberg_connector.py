# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Arrow Reader

Used to read datasets registered using the register_arrow or register_df functions.
"""

import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema

from opteryx.connectors import DiskConnector
from opteryx.connectors.base.base_connector import BaseConnector


class IcebergConnector(BaseConnector):
    __mode__ = "Blob"
    __type__ = "ARROW"

    def __init__(self, *args, catalog=None, io=DiskConnector, **kwargs):
        BaseConnector.__init__(self, **kwargs)

        self.dataset = self.dataset.lower()
        self.table = catalog.load_table(self.dataset)
        self.io_connector = io(**kwargs)

    def get_dataset_schema(self) -> RelationSchema:
        arrow_schema = self.table.schema().as_arrow()

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        return self.schema

    def read_dataset(self, columns: list = None, **kwargs) -> pyarrow.Table:
        if columns is None:
            column_names = self.schema.column_names
        else:
            column_names = [col.source_column for col in columns]

        reader = self.table.scan(
            selected_fields=column_names,
        ).to_arrow_batch_reader()

        for batch in reader:
            yield pyarrow.Table.from_batches([batch])
