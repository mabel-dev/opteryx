"""
Build different format files from a source JSONL file to test different format readers.
"""

# pragma: no cover

import orjson
import pyarrow.feather
import pyarrow.json
import pyarrow.orc
import pyarrow.parquet
import zstandard

import os, sys
sys.path.insert(1, os.path.join(sys.path[0], "../../../../draken"))

import glob
print(glob.glob(os.path.join(sys.path[0], "../../../../draken") + "/**"))

import orso


def compress_zstandard(records):  # pragma: no cover
    buffer = bytearray()

    for record in records:
        serialized = orjson.dumps(record) + b"\n"
        buffer.extend(serialized)

    buffer = zstandard.compress(buffer)

    return buffer


if __name__ == "__main__":  # pragma: no cover
    # READ (JSONL)
    with open("testdata/flat/formats/jsonl/tweets.jsonl", "rb") as stream:
        source = pyarrow.json.read_json(stream)

    # ARROW (feather)
#    pyarrow.feather.write_feather(source, "testdata/formats/arrow/tweets.arrow", compression="zstd")

    # ARROW (feather)
#    pyarrow.feather.write_feather(
#        source, "testdata/formats/arrow_lz4/tweets.arrow", compression="lz4"
#    )

    # ORC
#    pyarrow.orc.write_table(source, "testdata/formats/orc/tweets.orc", compression="ZSTD")

    # ORC
#    pyarrow.orc.write_table(source, "testdata/formats/orc_snappy/tweets.orc", compression="snappy")

    # PARQUET
#    pyarrow.parquet.write_table(
#        source, "testdata/formats/parquet/tweets.parquet", compression="zstd"
#    )

    # PARQUET
#    pyarrow.parquet.write_table(
#        source, "testdata/formats/parquet_snappy/tweets.parquet", compression="snappy"
#    )

    # PARQUET
#    pyarrow.parquet.write_table(
#        source, "testdata/formats/parquet_lz4/tweets.parquet", compression="lz4"
#    )

    # ZSTD
#    zstd = compress_zstandard(source.to_pylist())
#    with open("testdata/formats/zstd/tweets.zstd", "wb") as stream:
#        stream.write(zstd)
#    del zstd

    # DRAKEN
    from draken.compiled import create_sstable

    builder = {}
    df = orso.DataFrame.from_arrow(source)
    for i, r in enumerate(df):
        builder[str(i)] = r.values
    sstable_bytes = create_sstable(builder, df.schema.to_dict(), 0)
    with open(f"testdata/flat/formats/draken/tweets.draken", "wb") as f:
        f.write(sstable_bytes)