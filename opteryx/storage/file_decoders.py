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


def zstd_decoder(stream, projection):
    """
    Read zstandard compressed JSONL files
    """
    import zstandard

    with zstandard.open(stream, "rb") as file:
        return jsonl_decoder(file, projection)


def parquet_decoder(stream, projection):
    """
    Read parquet formatted files
    """
    import pyarrow.parquet as pq

    table = pq.read_table(stream, columns=projection)
    return table


def orc_decoder(stream, projection):
    """
    Read orc formatted files
    """
    import pyarrow.orc as orc

    orc_file = orc.ORCFile(stream)
    table = orc_file.read(columns=projection)
    return table


def jsonl_decoder(stream, projection):

    import pyarrow.json

    table = pyarrow.json.read_json(stream)

    # the read doesn't support projection, so do it now
    if projection:
        table = table.select(projection)

    return table


def arrow_decoder(stream, projection):

    import pyarrow.feather as pf

    table = pf.read_table(stream, columns=projection)
    return table
