# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Decode files from a raw binary format to a PyArrow Table.
"""

import io
from enum import Enum
from typing import BinaryIO
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

import pyarrow
from orso.tools import random_string
from orso.types import OrsoTypes
from pyarrow import parquet

from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.managers.expression import NodeType
from opteryx.managers.expression import get_all_nodes_of_type
from opteryx.utils.arrow import post_read_projector


class ExtentionType(str, Enum):
    """labels for the file extentions"""

    DATA = "DATA"
    CONTROL = "CONTROL"


def convert_avro_schema_to_orso_schema(avro_schema):
    from orso.schema import FlatColumn
    from orso.schema import RelationSchema

    avro_to_orso: Dict[str, OrsoTypes] = {
        "long": OrsoTypes.INTEGER,
        "string": OrsoTypes.VARCHAR,
        "timestamp": OrsoTypes.TIMESTAMP,
        "boolean": OrsoTypes.BOOLEAN,
        "array": OrsoTypes.ARRAY,
        "float": OrsoTypes.DOUBLE,
        "double": OrsoTypes.DOUBLE,
        "bytes": OrsoTypes.BLOB,
    }

    columns = []

    for column in avro_schema["fields"]:
        ct = None
        act = column.get("type")
        if isinstance(act, str):
            ct = avro_to_orso.get(act)
        if isinstance(act, list):
            types = [avro_to_orso.get(t) for t in act if t != "null"]
            if len(types) > 0:
                ct = types[0]
        if isinstance(act, dict):
            ct = avro_to_orso.get(act.get("type"))
        fc = FlatColumn(name=column.get("name"), type=ct)
        columns.append(fc)

    return RelationSchema(name=avro_schema.get("name"), columns=columns)


def convert_arrow_schema_to_orso_schema(arrow_schema):
    from orso.schema import FlatColumn
    from orso.schema import RelationSchema

    return RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )


def get_decoder(dataset: str) -> Callable:
    """helper routine to get the decoder for a given file"""
    ext = dataset.split(".")[-1].lower()
    if ext not in KNOWN_EXTENSIONS:  # pragma: no cover
        raise UnsupportedFileTypeError(f"Unsupported file type - {ext}")
    file_decoder, file_type = KNOWN_EXTENSIONS[ext]
    if file_type != ExtentionType.DATA:  # pragma: no cover
        raise UnsupportedFileTypeError(f"File is not a data file - {ext}")
    return file_decoder


def do_nothing(buffer: Union[memoryview, bytes], **kwargs):  # pragma: no cover
    """for when you need to look like you're doing something"""
    return False


def filter_records(filters: Optional[list], table: pyarrow.Table) -> pyarrow.Table:
    """
    Apply filters to a PyArrow table that could not be pushed down during the read operation.
    This is a post-read filtering step.

    Parameters:
        filters: Optional[list]
            A list of filter conditions (predicates) to apply to the table.
        table: pyarrow.Table
            The PyArrow table to be filtered.

    Returns:
        pyarrow.Table:
            A new PyArrow table with rows filtered according to the specified conditions.

    Note:
        At this point the columns are the raw column names from the file so we need to ensure
        the filters reference the raw column names not the engine internal 'identity'=
    """
    from opteryx.managers.expression import evaluate
    from opteryx.models import Node

    if isinstance(filters, list) and filters:
        # Create a copy of the filters list to avoid mutating the original.
        filter_copy = [f.copy() for f in filters]
        root = filter_copy.pop()

        # If the left or right side of the root filter node is an identifier, set its identity.
        # This step ensures that the filtering logic aligns with the schema before any renaming.
        if root.left.node_type == NodeType.IDENTIFIER:
            root.left.schema_column.identity = root.left.source_column
        if root.right.node_type == NodeType.IDENTIFIER:
            root.right.schema_column.identity = root.right.source_column

        while filter_copy:
            right = filter_copy.pop()
            if right.left.node_type == NodeType.IDENTIFIER:
                right.left.schema_column.identity = right.left.source_column
            if right.right.node_type == NodeType.IDENTIFIER:
                right.right.schema_column.identity = right.right.source_column
            # Combine the current root with the next filter using an AND node.
            root = Node(
                NodeType.AND,
                left=root,
                right=right,
                schema_column=Node("schema_column", identity=random_string()),
            )
    else:
        root = filters

    mask = evaluate(root, table)
    return table.filter(mask)


def zstd_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    """
    Read zstandard compressed JSONL files
    """
    import zstandard

    stream: BinaryIO = io.BytesIO(buffer)

    with zstandard.open(stream, "rb") as file:
        return jsonl_decoder(
            file, projection=projection, selection=selection, just_schema=just_schema
        )


def lzma_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    """
    Read lzma compressed JSONL files
    """
    import lzma

    stream: BinaryIO = io.BytesIO(buffer)

    with lzma.open(stream, "rb") as file:
        return jsonl_decoder(
            file, projection=projection, selection=selection, just_schema=just_schema
        )


def parquet_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    force_read: bool = False,
) -> Tuple[int, int, pyarrow.Table]:
    """
    Read parquet formatted files.

    Parameters:
        buffer: Union[memoryview, bytes]
            The input buffer containing the parquet file data.
        projection: List, optional
            List of columns to project.
        selection: optional
            The selection filter.
        just_schema: bool, optional
            Flag to indicate if only schema is needed.
        force_read: bool, optional
            Flag to skip some optimizations.
    Returns:
        Tuple containing number of rows, number of columns, and the table or schema.
    """
    # Open the parquet file only once
    stream = pyarrow.BufferReader(buffer)
    parquet_file = parquet.ParquetFile(stream)

    # Return just the schema if that's all that's needed
    if just_schema:
        return convert_arrow_schema_to_orso_schema(parquet_file.schema_arrow)

    # we need to work out if we have a selection which may force us
    # fetching columns just for filtering
    dnf_filter, processed_selection = (
        PredicatePushable.to_dnf(selection) if selection else (None, None)
    )

    if projection == [] and selection is None:
        # Create a boolean array with True values, one for each column in the Parquet file
        bool_array = pyarrow.array([True] * parquet_file.metadata.num_rows, type=pyarrow.bool_())
        # Create a PyArrow Table with the column name '*'
        table = pyarrow.Table.from_arrays([bool_array], ["*"])

        return (parquet_file.metadata.num_rows, 0, table)

    # Determine the columns needed for projection and filtering
    projection_set = set(p.source_column for p in projection or [])
    filter_columns = {
        c.value for c in get_all_nodes_of_type(processed_selection, (NodeType.IDENTIFIER,))
    }
    selected_columns = list(
        projection_set.union(filter_columns).intersection(parquet_file.schema_arrow.names)
    )

    # Read all columns if none are selected, unless force_read is set
    if not selected_columns and not force_read:
        selected_columns = []

    # Read the parquet table with the optimized column list and selection filters
    table = parquet.read_table(
        stream,
        columns=selected_columns,
        pre_buffer=False,
        filters=dnf_filter,
        use_threads=False,
        use_pandas_metadata=False,
        schema=parquet_file.schema_arrow,
    )

    # Any filters we couldn't push to PyArrow to read we run here
    if processed_selection:
        table = filter_records(processed_selection, table)

    return (parquet_file.metadata.num_rows, parquet_file.metadata.num_columns, table)


def orc_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    """
    Read orc formatted files
    """
    import pyarrow.orc as orc

    stream: BinaryIO = io.BytesIO(buffer)
    orc_file = orc.ORCFile(stream)

    if just_schema:
        orc_schema = orc_file.schema
        return convert_arrow_schema_to_orso_schema(orc_schema)

    table = orc_file.read()
    full_shape = table.shape
    if selection:
        table = filter_records(selection, table)
    if projection:
        table = post_read_projector(table, projection)
    return *full_shape, table


def jsonl_decoder(
    buffer: Union[memoryview, bytes, BinaryIO],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    import orjson
    import pyarrow.json
    import simdjson

    rows = []

    if not isinstance(buffer, bytes):
        buffer = buffer.read()  # type: ignore

    for line in buffer.split(b"\n"):
        if not line:
            continue
        dict_line = simdjson.Parser().parse(line)
        rows.append(
            {k: orjson.dumps(v) if isinstance(v, dict) else v for k, v in dict_line.items()}
        )

    table = pyarrow.Table.from_pylist(rows)

    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    full_shape = table.shape
    if selection:
        table = filter_records(selection, table)
    if projection:
        table = post_read_projector(table, projection)

    return *full_shape, table


def csv_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    delimiter: str = ",",
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    import pyarrow.csv
    from pyarrow.csv import ParseOptions

    stream: BinaryIO = io.BytesIO(buffer)
    parse_options = ParseOptions(delimiter=delimiter, newlines_in_values=True)
    table = pyarrow.csv.read_csv(stream, parse_options=parse_options)
    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    full_shape = table.shape
    if selection:
        table = filter_records(selection, table)
    if projection:
        table = post_read_projector(table, projection)

    return *full_shape, table


def tsv_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    return csv_decoder(
        buffer=buffer,
        projection=projection,
        selection=selection,
        delimiter="\t",
        just_schema=just_schema,
    )


def psv_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    return csv_decoder(
        buffer=buffer,
        projection=projection,
        selection=selection,
        delimiter="|",
        just_schema=just_schema,
    )


def arrow_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    import pyarrow.feather as pf

    stream: BinaryIO = io.BytesIO(buffer)
    table = pf.read_table(stream)
    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    full_shape = table.shape
    if selection:
        table = filter_records(selection, table)
    if projection:
        table = post_read_projector(table, projection)

    return *full_shape, table


def avro_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    """
    AVRO has a number of optimizations to make it faster than a naive implementation;
    the sample test script runs about 7x faster following these changes (the schema
    converter and selecting before convering to pyarrow).

    AVRO is still many many times slower than Parquet - it's not recommended as a
    bulk data format.
    """
    try:
        import fastavro
    except ImportError:  # pragma: no cover
        from opteryx.exceptions import MissingDependencyError

        raise MissingDependencyError("fastavro")

    stream: BinaryIO = io.BytesIO(buffer)
    reader = fastavro.reader(stream)

    if just_schema:
        # FastAvro exposes a schema we can convert without reading all the rows
        return convert_avro_schema_to_orso_schema(reader.schema)

    if projection:
        # It's almost always faster to avoid creating the column to convert in arrow
        # than creating and then removing them - although that would probably the fastest step
        projection = {c.value for c in projection}
        table = pyarrow.Table.from_pylist(
            [{k: v for k, v in row.items() if k in projection} for row in reader]
        )
    elif projection == []:
        # Empty table, we don't know the number of rows up front
        table = pyarrow.Table.from_arrays([[0 for r in reader]], ["_"])
    else:
        # Probably never run, convert every row and column to Arrow
        table = pyarrow.Table.from_pylist(list(reader))

    full_shape = table.shape
    if selection:
        # We can't push filters in Fast Avro, so filter here
        table = filter_records(selection, table)

    return *full_shape, table


def ipc_decoder(
    buffer: Union[memoryview, bytes],
    *,
    projection: Optional[list] = None,
    selection: Optional[list] = None,
    just_schema: bool = False,
    **kwargs,
) -> Tuple[int, int, pyarrow.Table]:
    from itertools import chain

    from pyarrow import ipc

    stream: BinaryIO = io.BytesIO(buffer)
    reader = ipc.open_stream(stream)

    batch_one = next(reader, None)
    if batch_one is None:
        return None

    schema = batch_one.schema

    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    table = pyarrow.Table.from_batches([batch for batch in chain([batch_one], reader)])
    full_shape = table.shape
    if selection:
        table = filter_records(selection, table)
    if projection:
        table = post_read_projector(table, projection)

    return *full_shape, table


# for types we know about, set up how we handle them
KNOWN_EXTENSIONS: Dict[str, Tuple[Callable, str]] = {
    "avro": (avro_decoder, ExtentionType.DATA),
    "complete": (do_nothing, ExtentionType.CONTROL),
    "manifest": (do_nothing, ExtentionType.CONTROL),
    "ignore": (do_nothing, ExtentionType.CONTROL),
    "arrow": (arrow_decoder, ExtentionType.DATA),  # feather
    "csv": (csv_decoder, ExtentionType.DATA),
    "ipc": (ipc_decoder, ExtentionType.DATA),
    "jsonl": (jsonl_decoder, ExtentionType.DATA),
    "orc": (orc_decoder, ExtentionType.DATA),
    "parquet": (parquet_decoder, ExtentionType.DATA),
    "tsv": (tsv_decoder, ExtentionType.DATA),
    "psv": (psv_decoder, ExtentionType.DATA),
    "zstd": (zstd_decoder, ExtentionType.DATA),  # jsonl/zstd
    "lzma": (lzma_decoder, ExtentionType.DATA),  # jsonl/lzma
}

VALID_EXTENSIONS = set(f".{ext}" for ext in KNOWN_EXTENSIONS)
TUPLE_OF_VALID_EXTENSIONS = tuple(VALID_EXTENSIONS)
DATA_EXTENSIONS = set(
    f".{ext}" for ext, conf in KNOWN_EXTENSIONS.items() if conf[1] == ExtentionType.DATA
)
