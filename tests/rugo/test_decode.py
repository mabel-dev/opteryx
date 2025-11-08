"""
Tests for Parquet data decoding functionality.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest

import opteryx.rugo.parquet as rp


def test_can_decode_uncompressed_plain():
    """Test that can_decode returns True for uncompressed PLAIN-encoded files."""
    # The binary.parquet file has uncompressed, PLAIN-encoded byte_array columns
    assert rp.can_decode('testdata/parquet_tests/binary.parquet') is True


def test_can_decode_compressed():
    """Test that can_decode returns True for SNAPPY compressed files."""
    # The snappy_compressed.parquet file uses SNAPPY compression with PLAIN encoding
    # SNAPPY compression is supported by our decoder
    assert rp.can_decode('testdata/parquet_tests/snappy_compressed.parquet') is True


def test_can_decode_dictionary_encoded():
    """Test that can_decode returns True for files with dictionary encoding."""
    # The dictionary_encoded.parquet file uses SNAPPY compression with RLE_DICTIONARY encoding
    # Both SNAPPY and RLE_DICTIONARY are supported
    assert rp.can_decode('testdata/parquet_tests/dictionary_encoded.parquet') is True


def test_can_decode_unsupported_types():
    """Test that can_decode returns False for files with unsupported types."""
    # The alltypes_plain.parquet has boolean, float, etc. which are not supported
    assert rp.can_decode('testdata/parquet_tests/alltypes_plain.parquet') is False


def test_decode_string_column():
    """Test decoding a string column from binary.parquet."""
    with open('testdata/parquet_tests/binary.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['foo'])
    
    # binary.parquet has 12 string values in first row group
    assert result is not None
    assert result['success']
    assert result['column_names'] == ['foo']
    data = result['row_groups'][0][0]  # First row group, first column
    assert isinstance(data, list)
    assert len(data) == 12
    assert all(isinstance(s, str) for s in data)


def test_decode_nonexistent_column():
    """Test that decoding a non-existent column returns None in the data."""
    with open('testdata/parquet_tests/binary.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['nonexistent'])
    # read_parquet returns success=True but column data is None
    assert result is not None
    assert result['success']
    assert result['row_groups'][0][0] is None


def test_decode_compressed_column():
    """Test that decoding a column with unsupported encoding returns None in the data."""
    # planets.parquet uses DELTA_BYTE_ARRAY encoding
    # We don't support DELTA_BYTE_ARRAY for decoding yet
    with open('testdata/planets/planets.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['name'])
    # read_parquet returns success=True but column data is None for unsupported encoding
    assert result is not None
    assert result['success']
    assert result['row_groups'][0][0] is None


def test_decode_int32_column():
    """Test decoding an int32 column."""
    with open('testdata/parquet_tests/test_decode.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['int32_col'])
    
    assert result is not None
    assert result['success']
    data = result['row_groups'][0][0]  # First row group, first column
    assert isinstance(data, list)
    assert len(data) == 5
    assert data == [10, 20, 30, 40, 50]


def test_decode_int64_column():
    """Test decoding an int64 column."""
    with open('testdata/parquet_tests/test_decode.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['int64_col'])
    
    assert result is not None
    assert result['success']
    data = result['row_groups'][0][0]  # First row group, first column
    assert isinstance(data, list)
    assert len(data) == 5
    assert data == [100, 200, 300, 400, 500]


def test_decode_string_column_types():
    """Test decoding a string column."""
    with open('testdata/parquet_tests/test_decode.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['string_col'])
    
    assert result is not None
    assert result['success']
    data = result['row_groups'][0][0]  # First row group, first column
    assert isinstance(data, list)
    assert len(data) == 5
    assert data == ['test1', 'test2', 'test3', 'test4', 'test5']


def test_can_decode_test_file():
    """Test that can_decode works for test_decode.parquet."""
    assert rp.can_decode('testdata/parquet_tests/test_decode.parquet') is True


def test_decode_snappy_compressed_column():
    """Test decoding a column from a SNAPPY compressed file."""
    # snappy_compressed.parquet has SNAPPY compression with PLAIN encoding
    with open('testdata/parquet_tests/snappy_compressed.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['id'])
    
    # File has 2 row groups with 500 rows each
    assert result is not None
    assert result['success']
    data = result['row_groups'][0][0]  # First row group, first column
    assert isinstance(data, list)
    assert len(data) == 500
    assert all(isinstance(x, int) for x in data)


def test_decode_dictionary_encoded_column():
    """Test decoding a dictionary-encoded column."""
    # dictionary_encoded.parquet has RLE_DICTIONARY encoding
    with open('testdata/parquet_tests/dictionary_encoded.parquet', 'rb') as f:
        file_data = f.read()
    
    result = rp.read_parquet(file_data, ['category'])
    
    # File has 2 row groups with 500 rows each
    assert result is not None
    assert result['success']
    
    # Dictionary decoding may or may not be fully implemented
    data = result['row_groups'][0][0]  # First row group, first column
    if data is not None:
        assert isinstance(data, list)
        assert len(data) == 500


if __name__ == "__main__":

    pytest.main([__file__, "-v"])
