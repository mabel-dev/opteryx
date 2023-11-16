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
Decode files from a raw binary format to a PyArrow Table.
"""
import io
from enum import Enum
from typing import Callable
from typing import List

import numpy
import pyarrow

from opteryx.exceptions import UnsupportedFileTypeError
from opteryx.utils.arrow import post_read_projector

memory_pool = pyarrow.default_memory_pool()


class ExtentionType(str, Enum):
    """labels for the file extentions"""

    DATA = "DATA"
    CONTROL = "CONTROL"


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
    if ext not in KNOWN_EXTENSIONS:
        raise UnsupportedFileTypeError(f"Unsupported file type - {ext}")
    file_decoder, file_type = KNOWN_EXTENSIONS[ext]
    if file_type != ExtentionType.DATA:
        raise UnsupportedFileTypeError(f"File is not a data file - {ext}")
    return file_decoder


def do_nothing(buffer, projection=None, just_schema: bool = False):  # pragma: no-cover
    """for when you need to look like you're doing something"""
    return False


def filter_records(filter, table):
    """
    When we can't push predicates to the actual read, use this to filter records
    just after the read.
    """
    # notes:
    #   at this point we've not renamed any columns, this may affect some filters
    from opteryx.managers.expression import evaluate

    mask = evaluate(filter, table, False)
    return table.filter(mask)


def zstd_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    """
    Read zstandard compressed JSONL files
    """
    import zstandard

    stream = io.BytesIO(buffer)

    with zstandard.open(stream, "rb") as file:
        return jsonl_decoder(
            file, projection=projection, selection=selection, just_schema=just_schema
        )


def parquet_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    """
    Read parquet formatted files.
    """
    from pyarrow import parquet

    from opteryx.connectors.capabilities import predicate_pushable

    # Convert the selection to DNF format if applicable
    _select = predicate_pushable.to_dnf(selection) if selection is not None else None

    stream = io.BytesIO(buffer)

    if projection or just_schema:
        # Open the parquet file only once
        parquet_file = parquet.ParquetFile(stream)

        # Return just the schema if that's all that's needed
        if just_schema:
            return convert_arrow_schema_to_orso_schema(parquet_file.schema_arrow)

        # Special handling for projection of ["count_*"]
        if projection == ["count_*"]:
            return pyarrow.Table.from_pydict(
                {"_": numpy.full(parquet_file.metadata.num_rows, True, dtype=numpy.bool_)}
            )

        # Projection processing
        schema_columns_set = set(parquet_file.schema_arrow.names)
        projection_names = {name for proj_col in projection for name in proj_col.all_names}
        selected_columns = list(schema_columns_set.intersection(projection_names))

        # If no columns are selected, set to None
        if not selected_columns:
            selected_columns = None
    else:
        selected_columns = None

    # Read the parquet table with the optimized column list and selection filters
    return parquet.read_table(stream, columns=selected_columns, pre_buffer=False, filters=_select)


def orc_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    """
    Read orc formatted files
    """
    import pyarrow.orc as orc

    stream = io.BytesIO(buffer)
    orc_file = orc.ORCFile(stream)
    orc_schema = orc_file.schema
    schema_columns = orc_schema.names
    if just_schema:
        return convert_arrow_schema_to_orso_schema(orc_schema)

    # work out the selected columns, handling aliases
    selected_columns = []
    if projection:
        for projection_column in projection:
            for schema_column in schema_columns:
                if schema_column in projection_column.all_names:
                    selected_columns.append(schema_column)
                    break
    if len(selected_columns) == 0:  # pragma: no-cover
        selected_columns = None

    table = orc_file.read(columns=selected_columns)
    if selection is not None:
        table = filter_records(selection, table)
    return table


def jsonl_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    import pyarrow.json

    if isinstance(buffer, bytes):
        stream = io.BytesIO(buffer)
    else:
        stream = buffer

    table = pyarrow.json.read_json(stream)
    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    if projection:
        table = post_read_projector(table, projection)

    return table


def csv_decoder(
    buffer, projection: List = None, selection=None, delimiter: str = ",", just_schema: bool = False
):
    import pyarrow.csv
    from pyarrow.csv import ParseOptions
    from pyarrow.csv import ReadOptions

    stream = io.BytesIO(buffer)
    parse_options = ParseOptions(delimiter=delimiter, newlines_in_values=True)
    table = pyarrow.csv.read_csv(stream, parse_options=parse_options)
    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    if projection:
        table = post_read_projector(table, projection)

    return table


def tsv_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    return csv_decoder(
        buffer=buffer,
        projection=projection,
        selection=selection,
        delimiter="\t",
        just_schema=just_schema,
    )


def arrow_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    import pyarrow.feather as pf

    stream = io.BytesIO(buffer)
    table = pf.read_table(stream)
    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    if projection:
        table = post_read_projector(table, projection)

    return table


def avro_decoder(buffer, projection: List = None, selection=None, just_schema: bool = False):
    """
    AVRO isn't well supported, it is converted between table types which is
    really slow
    """
    try:
        from avro.datafile import DataFileReader
        from avro.io import DatumReader
    except ImportError:  # pragma: no-cover
        raise Exception("`avro` is missing, please install or include in your `requirements.txt`.")

    stream = io.BytesIO(buffer)
    reader = DataFileReader(stream, DatumReader())

    table = pyarrow.Table.from_pylist(list(reader))
    schema = table.schema
    if just_schema:
        return convert_arrow_schema_to_orso_schema(schema)

    if projection:
        table = post_read_projector(table, projection)

    return table


# for types we know about, set up how we handle them
KNOWN_EXTENSIONS = {
    "avro": (avro_decoder, ExtentionType.DATA),
    "complete": (do_nothing, ExtentionType.CONTROL),
    "ignore": (do_nothing, ExtentionType.CONTROL),
    "arrow": (arrow_decoder, ExtentionType.DATA),  # feather
    "csv": (csv_decoder, ExtentionType.DATA),
    "jsonl": (jsonl_decoder, ExtentionType.DATA),
    "orc": (orc_decoder, ExtentionType.DATA),
    "parquet": (parquet_decoder, ExtentionType.DATA),
    "tsv": (tsv_decoder, ExtentionType.DATA),
    "zstd": (zstd_decoder, ExtentionType.DATA),  # jsonl/zstd
}

VALID_EXTENSIONS = set(f".{ext}" for ext in KNOWN_EXTENSIONS.keys())
DATA_EXTENSIONS = set(
    f".{ext}" for ext, conf in KNOWN_EXTENSIONS.items() if conf[1] == ExtentionType.DATA
)
