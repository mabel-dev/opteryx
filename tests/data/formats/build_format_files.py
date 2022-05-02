"""
Build different format files from a source JSONL file to test different format readers.
"""
import orjson
from regex import P
import zstandard
import pyarrow.json
import pyarrow.feather
import pyarrow.orc
import pyarrow.parquet


def compress_zstandard(records):

    buffer = bytearray()

    for record in records:
        serialized = orjson.dumps(record) + b"\n"
        buffer.extend(serialized)

    buffer = zstandard.compress(buffer)

    return buffer


# READ (JSONL)
with open("tests/data/formats/jsonl/tweets.jsonl", "rb") as stream:
    source = pyarrow.json.read_json(stream)

# ARROW (feather)
pyarrow.feather.write_feather(
    source, "tests/data/formats/arrow/tweets.arrow", compression="zstd"
)

# AVRO

# ORC
pyarrow.orc.write_table(source, "tests/data/formats/orc/tweets.orc", compression="ZSTD")

# PARQUET
pyarrow.parquet.write_table(
    source, "tests/data/formats/parquet/tweets.parquet", compression="zstd"
)

# ZSTD
zstd = compress_zstandard(source.to_pylist())
with open("tests/data/formats/zstd/tweets.zstd", "wb") as stream:
    stream.write(zstd)
del zstd
