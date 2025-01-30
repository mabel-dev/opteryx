# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Iceberg Connector
"""

import datetime
import struct
from decimal import Decimal
from typing import Dict
from typing import Union

import numpy
import pyarrow
from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.connectors import DiskConnector
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import LimitPushable
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.connectors.capabilities import Statistics
from opteryx.exceptions import NotSupportedError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.models import RelationStatistics
from opteryx.utils.file_decoders import filter_records


@single_item_cache
def to_iceberg_filter(root):
    """
    Convert a filter to Iceberg filter form.

    This is specifically opinionated for the Iceberg reader.
    """
    import pyiceberg
    import pyiceberg.expressions

    ICEBERG_FILTERS = {
        "GtEq": pyiceberg.expressions.GreaterThanOrEqual,
        "Eq": pyiceberg.expressions.EqualTo,
        "NotEq": pyiceberg.expressions.NotEqualTo,
        "Gt": pyiceberg.expressions.GreaterThan,
        "Lt": pyiceberg.expressions.LessThan,
        "LtEq": pyiceberg.expressions.LessThanOrEqual,
    }

    def _predicate_to_iceberg_filter(root):
        # Reduce look-ahead effort by using Exceptions to control flow
        if root.node_type == NodeType.AND:  # pragma: no cover
            left = _predicate_to_iceberg_filter(root.left)
            right = _predicate_to_iceberg_filter(root.right)
            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]
            left.extend(right)
            return left
        if root.node_type != NodeType.COMPARISON_OPERATOR:
            raise NotSupportedError()
        if root.left.node_type != NodeType.IDENTIFIER:
            root.left, root.right = root.right, root.left
        if root.right.schema_column.type == OrsoTypes.DATE:
            date_val = root.right.value
            if hasattr(date_val, "item"):
                date_val = date_val.item()
            root.right.value = datetime.datetime.combine(date_val, datetime.time.min)
            root.right.schema_column.type = OrsoTypes.TIMESTAMP
        if root.left.schema_column.type == OrsoTypes.DATE:
            root.left.schema_column.type = OrsoTypes.TIMESTAMP
        if root.left.node_type != NodeType.IDENTIFIER:
            raise NotSupportedError()
        if root.right.node_type != NodeType.LITERAL:
            raise NotSupportedError()
        if root.left.schema_column.type == OrsoTypes.VARCHAR:
            root.left.schema_column.type = OrsoTypes.BLOB
        if root.right.schema_column.type == OrsoTypes.VARCHAR:
            root.right.schema_column.type = OrsoTypes.BLOB
        if root.right.schema_column.type == OrsoTypes.DOUBLE:
            # iceberg needs doubles to be cast to floats
            root.right.value = float(root.right.value)
        if root.right.schema_column.type == OrsoTypes.INTEGER:
            # iceberg doesn't like integers unless we convert to strings
            root.right.value = str(root.right.value)
        if root.right.schema_column.type == OrsoTypes.TIMESTAMP:
            # iceberg doesn't like timestamps unless we convert to strings
            if isinstance(root.right.value, numpy.datetime64):
                root.right.value = root.right.value.astype(datetime.datetime)
            root.right.value = root.right.value.isoformat()
        if root.right.schema_column.type != root.left.schema_column.type:
            raise NotSupportedError(
                f"{root.right.schema_column.type} != {root.left.schema_column.type}"
            )
        return ICEBERG_FILTERS[root.value](root.left.value, root.right.value)

    iceberg_filter = None
    unsupported = []
    if not isinstance(root, list):
        root = [root]
    for predicate in root:
        try:
            converted = _predicate_to_iceberg_filter(predicate)
            if iceberg_filter is None:
                iceberg_filter = converted
            else:
                iceberg_filter = pyiceberg.expressions.And(iceberg_filter, converted)
        except NotSupportedError:
            unsupported.append(predicate)

    return iceberg_filter if iceberg_filter else "True", unsupported


class IcebergConnector(BaseConnector, LimitPushable, Statistics, PredicatePushable):
    __mode__ = "Blob"
    __type__ = "ICEBERG"

    PUSHABLE_OPS: Dict[str, bool] = {
        "Eq": True,
        "NotEq": True,
        "Gt": True,
        "GtEq": True,
        "Lt": True,
        "LtEq": True,
    }

    PUSHABLE_TYPES = {
        OrsoTypes.BLOB,
        OrsoTypes.BOOLEAN,
        OrsoTypes.DOUBLE,
        OrsoTypes.INTEGER,
        OrsoTypes.VARCHAR,
        OrsoTypes.TIMESTAMP,
        OrsoTypes.DATE,
    }

    def __init__(self, *args, catalog=None, io=DiskConnector, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        LimitPushable.__init__(self, **kwargs)
        Statistics.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

        self.dataset = self.dataset.lower()
        self.table = catalog.load_table(self.dataset)
        self.io_connector = io(**kwargs)

    def get_dataset_schema(self) -> RelationSchema:
        iceberg_schema = self.table.schema()
        arrow_schema = iceberg_schema.as_arrow()

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        # Get statistics
        relation_statistics = RelationStatistics()

        column_names = {col.field_id: col.name for col in iceberg_schema.columns}
        column_types = {col.field_id: col.field_type for col in iceberg_schema.columns}

        files = self.table.inspect.files()
        relation_statistics.record_count = pyarrow.compute.sum(files.column("record_count")).as_py()

        if "distinct_counts" in files.columns:
            for file in files.column("distinct_counts"):
                for k, v in file:
                    relation_statistics.set_cardinality_estimate(column_names[k], v)

        if "value_counts" in files.columns:
            for file in files.column("value_counts"):
                for k, v in file:
                    relation_statistics.add_count(column_names[k], v)

        for file in files.column("lower_bounds"):
            for k, v in file:
                relation_statistics.update_lower(
                    column_names[k], IcebergConnector.decode_iceberg_value(v, column_types[k])
                )

        for file in files.column("upper_bounds"):
            for k, v in file:
                relation_statistics.update_upper(
                    column_names[k], IcebergConnector.decode_iceberg_value(v, column_types[k])
                )

        self.relation_statistics = relation_statistics

        return self.schema

    def read_dataset(
        self, columns: list = None, predicates: list = None, limit: int = None, **kwargs
    ) -> pyarrow.Table:
        rows_read = 0

        if columns is None:
            column_names = self.schema.column_names
        else:
            column_names = [col.source_column for col in columns]

        pushed_filters, unsupported = to_iceberg_filter(predicates)
        selected_columns = list(
            set(column_names).union(
                {
                    c.source_column
                    for c in get_all_nodes_of_type(unsupported, (NodeType.IDENTIFIER,))
                }
            )
        )

        reader = self.table.scan(
            row_filter=pushed_filters, selected_fields=selected_columns, limit=limit
        ).to_arrow_batch_reader()

        batch = None
        for batch in reader:
            table = pyarrow.Table.from_batches([batch])
            if unsupported:
                table = filter_records(unsupported, table)
            yield table
            rows_read += batch.num_rows

        if batch is None:
            from orso.schema import RelationSchema
            from orso.schema import convert_orso_schema_to_arrow_schema

            orso_schema = RelationSchema(
                name="Relation", columns=[c.schema_column for c in columns]
            )
            arrow_shema = convert_orso_schema_to_arrow_schema(orso_schema, use_identities=True)

            morsel = pyarrow.Table.from_arrays(
                [pyarrow.array([]) for _ in columns],
                schema=arrow_shema,
            )
            yield morsel

    @staticmethod
    def decode_iceberg_value(
        value: Union[int, float, bytes], data_type: str, scale: int = None
    ) -> Union[int, float, str, datetime.datetime, Decimal, bool]:
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

        if data_type_class == pyiceberg.types.LongType:
            return int.from_bytes(value, "little", signed=True)
        elif data_type_class == pyiceberg.types.DoubleType:
            # IEEE 754 encoded floats are typically decoded directly
            return struct.unpack("<d", value)[0]  # 8-byte IEEE 754 double
        elif data_type_class == pyiceberg.types.TimestampType:
            # Iceberg stores timestamps as microseconds since epoch
            interval = int.from_bytes(value, "little", signed=True)
            return datetime.datetime.fromtimestamp(interval / 1_000_000)
        elif data_type == "date":
            # Iceberg stores dates as days since epoch (1970-01-01)
            interval = int.from_bytes(value, "little", signed=True)
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(days=interval)
        elif data_type_class == pyiceberg.types.StringType:
            # Assuming UTF-8 encoded bytes (or already decoded string)
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
        elif data_type_class == pyiceberg.types.BinaryType:
            return value
        elif str(data_type).startswith("decimal"):
            # Iceberg stores decimals as unscaled integers
            int_value = int.from_bytes(value, byteorder="big", signed=True)
            return Decimal(int_value) / (10**data_type.scale)
        elif data_type_class == pyiceberg.types.BooleanType:
            return bool(value)
        else:
            raise ValueError(f"Unsupported data type: {data_type}, {str(data_type)}")
