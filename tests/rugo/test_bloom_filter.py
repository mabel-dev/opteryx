from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pytest

from opteryx.rugo import parquet

DATASET = Path("testdata/parquet_tests/data_index_bloom_encoding_stats.parquet")


def _bloom_info():
    metadata = parquet.read_metadata(os.fspath(DATASET))
    column = metadata["row_groups"][0]["columns"][0]
    return column["bloom_offset"], column["bloom_length"]


def test_bloom_filter_detects_present_value():
    offset, length = _bloom_info()
    assert offset is not None

    assert parquet.test_bloom_filter(DATASET, offset, length, "Hello")
    assert parquet.test_bloom_filter(DATASET, offset, length, b"This is")
    assert parquet.test_bloom_filter(DATASET, offset, None, "a")


def test_bloom_filter_rejects_absent_value():
    offset, length = _bloom_info()
    assert not parquet.test_bloom_filter(DATASET, offset, length, "missing item")
    assert not parquet.test_bloom_filter(DATASET, offset, length, b"totally unknown")


def test_bloom_filter_validates_offset():
    offset, length = _bloom_info()
    with pytest.raises(ValueError):
        parquet.test_bloom_filter(DATASET, -1, length, "Hello")
    with pytest.raises(ValueError):
        parquet.test_bloom_filter(DATASET, None, length, "Hello")

if __name__ == "__main__":
    pytest.main([__file__])
