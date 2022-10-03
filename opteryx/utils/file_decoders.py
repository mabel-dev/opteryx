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
from typing import List

import numpy
import pyarrow


def zstd_decoder(stream, projection: List = None):
    """
    Read zstandard compressed JSONL files
    """
    import zstandard

    with zstandard.open(stream, "rb") as file:
        return jsonl_decoder(file, projection)


def parquet_decoder(stream, projection: List = None):
    """
    Read parquet formatted files
    """
    from pyarrow import parquet

    selected_columns = None
    if isinstance(projection, (list, set)) and "*" not in projection:
        # if we have a pushed down projection, get the list of columns from the file
        # and then only set the reader to read those
        parquet_file = parquet.ParquetFile(stream)
        # .schema_arrow is probably slower than .schema but there are instances of
        # .schema being incomplete #468
        parquet_metadata = parquet_file.schema_arrow

        if projection == {"count_*"}:
            return pyarrow.Table.from_pydict(
                {
                    "_": numpy.full(
                        parquet_file.metadata.num_rows, True, dtype=numpy.bool_
                    )
                }
            )

        selected_columns = list(set(parquet_metadata.names).intersection(projection))
        # if nothing matched, there's been a problem - maybe HINTS confused for columns
        if len(selected_columns) == 0:
            selected_columns = None
    # don't prebuffer - we're already buffered as an IO Stream
    return parquet.read_table(stream, columns=selected_columns, pre_buffer=False)


def orc_decoder(stream, projection: List = None):
    """
    Read orc formatted files
    """
    import pyarrow.orc as orc

    orc_file = orc.ORCFile(stream)

    selected_columns = None
    if isinstance(projection, (list, set)) and "*" not in projection:
        orc_metadata = orc_file.schema
        selected_columns = list(set(orc_metadata.names).intersection(projection))
        # if nothing matched, there's been a problem - maybe HINTS confused for columns
        if len(selected_columns) == 0:
            selected_columns = None

    table = orc_file.read()
    return table


def jsonl_decoder(stream, projection: List = None):

    import pyarrow.json

    table = pyarrow.json.read_json(stream)

    # the read doesn't support projection, so do it now
    if projection and "*" not in projection:
        selected_columns = list(set(table.column_names).intersection(projection))
        # if nothing matched, don't do a thing
        if len(selected_columns) > 0:
            table = table.select(selected_columns)

    return table


def csv_decoder(stream, projection: List = None):

    import pyarrow.csv

    table = pyarrow.csv.read_csv(stream)

    # the read doesn't support projection, so do it now
    if projection and "*" not in projection:
        selected_columns = list(set(table.column_names).intersection(projection))
        # if nothing matched, don't do a thing
        if len(selected_columns) > 0:
            table = table.select(selected_columns)

    return table


def arrow_decoder(stream, projection: List = None):

    import pyarrow.feather as pf

    table = pf.read_table(stream)

    # we can't get the schema before reading the file, so do selection now
    if projection and "*" not in projection:
        selected_columns = list(set(table.column_names).intersection(projection))
        # if nothing matched, don't do a thing
        if len(selected_columns) > 0:
            table = table.select(selected_columns)

    return table
