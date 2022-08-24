"""
Performance tests are not intended to be ran as part of the regression set.

This tests the relative performance of different storage formats.

100 cycles of arrow took 78.264939371 seconds
100 cycles of jsonl took 90.697849402 seconds
100 cycles of orc took 79.319735311 seconds
100 cycles of parquet took 84.210948929 seconds
100 cycles of zstd took 92.060245531 seconds
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.storage.adapters.blob import DiskStorage
from opteryx.storage.cache.memory_cache import InMemoryCache

import time


class Timer(object):
    def __init__(self, name="test"):
        self.name = name

    def __enter__(self):
        self.start = time.time_ns()

    def __exit__(self, type, value, traceback):
        print(
            "{} took {} seconds".format(self.name, (time.time_ns() - self.start) / 1e9)
        )


FORMATS = (
    "arrow",
    "arrow_lz4",
    "jsonl",
    "orc",
    "orc_snappy",
    "parquet",
    "parquet_snappy",
    "parquet_lz4",
    "zstd",
)
cache = InMemoryCache(size=100)

if __name__ == "__main__":

    CYCLES = 25

    opteryx.storage.register_prefix("tests", DiskStorage)

    conn = opteryx.connect(cache=cache)

    for format in FORMATS:
        with Timer(f"{CYCLES} cycles of {format}"):
            for round in range(CYCLES):
                cur = conn.cursor()
                cur.execute(
                    f"SELECT followers FROM tests.data.formats.{format} WITH(NO_PARTITION);"
                )
                #                [a for a in cur._results]
                [a for a in cur.fetchall()]
