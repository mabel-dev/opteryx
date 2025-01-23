# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Arrow Reader

Used to read datasets registered using the register_arrow or register_df functions.
"""

from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Union

import pyarrow
import pyiceberg.typedef
import pyiceberg.types
from orso.schema import FlatColumn
from orso.schema import RelationSchema

from opteryx.connectors import DiskConnector
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import LimitPushable


class IcebergConnector(BaseConnector, LimitPushable):
    __mode__ = "Blob"
    __type__ = "ICEBERG"

    def __init__(self, *args, catalog=None, io=DiskConnector, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)

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
        rows_read = 0
        limit = kwargs.get("limit")

        if columns is None:
            column_names = self.schema.column_names
        else:
            column_names = [col.source_column for col in columns]

        reader = self.table.scan(
            selected_fields=column_names,
        ).to_arrow_batch_reader()

        for batch in reader:
            if limit and rows_read + batch.num_rows > limit:
                batch = batch.slice(0, limit - rows_read)
            yield pyarrow.Table.from_batches([batch])
            rows_read += batch.num_rows
            if limit and rows_read >= limit:
                break

    @staticmethod
    def decode_iceberg_value(
        value: Union[int, float, bytes], data_type: str, scale: int = None
    ) -> Union[int, float, str, datetime, Decimal, bool]:
        """
        Decode Iceberg-encoded values based on the specified data type.

        Parameters:
            value: Union[int, float, bytes]
                The encoded value from Iceberg.
            data_type: str
                The type of the value ('int', 'long', 'float', 'double', 'timestamp', 'date', 'string', 'decimal', 'boolean').
            scale: int, optional
                Scale used for decoding decimal types, defaults to None.

        Returns:
            The decoded value in its original form.
        """
        import pyiceberg

        data_type_class = data_type.__class__

        if data_type_class in (pyiceberg.types.LongType,):
            return int.from_bytes(value, "little", signed=True)
        elif data_type in {"float", "double"}:
            # IEEE 754 encoded floats are typically decoded directly
            return float(value)
        elif data_type == "timestamp":
            # Iceberg stores timestamps as microseconds since epoch
            return datetime.fromtimestamp(value / 1_000_000)
        elif data_type == "date":
            # Iceberg stores dates as days since epoch (1970-01-01)
            return datetime(1970, 1, 1) + timedelta(days=value)
        elif data_type_class == pyiceberg.types.StringType:
            # Assuming UTF-8 encoded bytes (or already decoded string)
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
        elif data_type == "decimal":
            # Iceberg stores decimals as unscaled integers
            if scale is None:
                raise ValueError("Scale must be provided for decimal decoding.")
            return Decimal(value) / (10**scale)
        elif data_type_class == pyiceberg.types.BooleanType:
            return bool(value)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
