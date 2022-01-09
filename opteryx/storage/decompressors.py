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
Decompressors for the Relation based Readers.

These return a tuple of ()
"""

from opteryx.exceptions import MissingDependencyError

import simdjson

pass_thru = lambda x: x
json_parser = simdjson.Parser()


def _json_to_tuples(line):
    """
    Parse each line in the file to a dictionary.

    We do some juggling so we can delete the object which is faster than creating a
    new Parser for each record.
    """
    dic = json_parser.parse(line)
    keys = tuple(dic.keys())
    values = tuple(dic.values())
    del dic
    return keys, values


def zstd_reader(stream):
    """
    Read zstandard compressed files

    This assumes the underlying format is line separated records and each record is
    the same.
    """
    import zstandard

    with zstandard.open(stream, "rb") as file:
        yield from map(_json_to_tuples, file.read().split(b"\n")[:-1])


def parquet_reader(stream):
    """
    Read parquet formatted files
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`pyarrow` is missing, please install or include in requirements.txt"
        )
    table = pq.read_table(stream)
    for batch in table.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            yield tuple([v[index] for k, v in dict_batch.items()])  # yields a tuple


def orc_reader(stream):
    """
    Read orc formatted files
    """
    try:
        import pyarrow.orc as orc
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`pyarrow` is missing, please install or include in requirements.txt"
        )

    orc_file = orc.ORCFile(stream)
    data = orc_file.read()  # columns=[] to push down projection

    for batch in data.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            yield tuple([v[index] for k, v in dict_batch.items()])  # yields a tuple


def jsonl_reader(stream):
    """
    Assumes a record by line
    """
    text = stream.read()  # type:ignore
    yield from map(_json_to_tuples, text.splitlines())
