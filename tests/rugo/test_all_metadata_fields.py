"""
Test that all metadata fields from the C++ ColumnStats struct are exposed to Python.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import glob
import pytest

from opteryx.rugo import parquet


def test_all_metadata_fields_exposed():
    """Test that all C++ ColumnStats fields are exposed in the Python dictionary."""
    # Read a test file

    files_to_test = glob.glob("testdata/*.parquet", recursive=True)

    for file_path in files_to_test:

        print(f"\nTesting file: {file_path}")

        metadata = parquet.read_metadata(file_path)

        # Get first column metadata
        assert metadata['row_groups'], "No row groups found"
        assert metadata['row_groups'][0]['columns'], "No columns found"
        col = metadata['row_groups'][0]['columns'][0]
        
        # Define all expected fields from ColumnStats struct
        expected_fields = {
            # Basic fields
            'name',
            'physical_type',
            'logical_type',
            'path_in_schema',  # not always present
            
            # Sizes & counts
            'num_values',
            'total_uncompressed_size',
            'total_compressed_size',
            
            # Offsets
            'data_page_offset',
            'index_page_offset',
            'dictionary_page_offset',
            
            # Statistics
            'min',
            'max',
            'null_count',
            'distinct_count',
            
            # Bloom filter
            'bloom_offset',
            'bloom_length',
            
            # Encodings & codec
            'encodings',
            'compression_codec',  # codec in C++
            
            # Key/value metadata
            'key_value_metadata',
        }
        
        # Check that all expected fields are present
        actual_fields = set(col.keys())
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields
        
        assert not missing_fields, f"Missing fields in column metadata: {missing_fields}"
        assert not extra_fields, f"Unexpected fields in column metadata: {extra_fields}"
        
        print(f"✅ All {len(expected_fields)} expected fields are present in column metadata")


def test_metadata_field_types():
    """Test that metadata fields have the correct types."""
    files_to_test = glob.glob("testdata/*.parquet", recursive=True)

    for file_path in files_to_test:

        metadata = parquet.read_metadata(file_path)
        col = metadata['row_groups'][0]['columns'][0]
        
        # Check types
        assert isinstance(col['name'], str)
        assert isinstance(col['physical_type'], str)
        assert isinstance(col['logical_type'], str)
        
        # These can be int or None
        assert col['num_values'] is None or isinstance(col['num_values'], int)
        assert col['total_uncompressed_size'] is None or isinstance(col['total_uncompressed_size'], int)
        assert col['total_compressed_size'] is None or isinstance(col['total_compressed_size'], int)
        assert col['data_page_offset'] is None or isinstance(col['data_page_offset'], int)
        assert col['index_page_offset'] is None or isinstance(col['index_page_offset'], int)
        assert col['dictionary_page_offset'] is None or isinstance(col['dictionary_page_offset'], int)
        assert col['null_count'] is None or isinstance(col['null_count'], int)
        assert col['distinct_count'] is None or isinstance(col['distinct_count'], int)
        assert col['bloom_offset'] is None or isinstance(col['bloom_offset'], int)
        assert col['bloom_length'] is None or isinstance(col['bloom_length'], int)
        
        # Encodings should be a list of strings
        assert isinstance(col['encodings'], list)
        assert all(isinstance(enc, str) for enc in col['encodings'])
        
        # Compression codec should be string or None
        assert col['compression_codec'] is None or isinstance(col['compression_codec'], str)
        
        # Key/value metadata should be dict or None
        assert col['key_value_metadata'] is None or isinstance(col['key_value_metadata'], dict)
        
        print("✅ All field types are correct")


def test_metadata_field_values():
    """Test that metadata field values are reasonable."""
    files_to_test = glob.glob("testdata/*.parquet", recursive=True)

    for file_path in files_to_test:

        metadata = parquet.read_metadata(file_path)
        col = metadata['row_groups'][0]['columns'][0]
        
        # Basic fields should be present
        assert col.get('name') is not None
        assert col.get('physical_type') is not None
        assert col.get('logical_type') is not None
        
        # Sizes should be positive if present
        if col['num_values'] is not None:
            assert col['num_values'] > 0
        if col['total_uncompressed_size'] is not None:
            assert col['total_uncompressed_size'] > 0
        if col['total_compressed_size'] is not None:
            assert col['total_compressed_size'] > 0
        
        # Offsets should be non-negative if present
        if col['data_page_offset'] is not None:
            assert col['data_page_offset'] >= 0
        if col['index_page_offset'] is not None:
            assert col['index_page_offset'] >= 0
        if col['dictionary_page_offset'] is not None:
            assert col['dictionary_page_offset'] >= 0
        
        # Encodings should be non-empty
        assert len(col['encodings']) > 0
        
        # Compression codec should be a known value if present
        if col['compression_codec'] is not None:
            known_codecs = {'UNCOMPRESSED', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD', 'LZ4_RAW', 'UNKNOWN'}
            assert col['compression_codec'] in known_codecs, f"Unknown codec: {col['compression_codec']}"
        
        print("✅ Field values are reasonable")
        print(f"   - Name: {col['name']}")
        print(f"   - Type: {col['physical_type']} ({col['logical_type']})")
        print(f"   - Num values: {col['num_values']}")
        print(f"   - Compressed size: {col['total_compressed_size']} bytes")
        print(f"   - Uncompressed size: {col['total_uncompressed_size']} bytes")
        print(f"   - Encodings: {col['encodings']}")
        print(f"   - Codec: {col['compression_codec']}")


def test_multiple_columns():
    """Test that all columns have the complete metadata."""
    metadata = parquet.read_metadata('testdata/planets/planets.parquet')
    
    expected_fields = {
        'name', 'physical_type', 'logical_type', 'num_values', 'total_uncompressed_size',
        'total_compressed_size', 'data_page_offset', 'index_page_offset',
        'dictionary_page_offset', 'min', 'max', 'null_count', 'distinct_count',
        'bloom_offset', 'bloom_length', 'encodings', 'compression_codec',
        'key_value_metadata'
    }
    
    for col in metadata['row_groups'][0]['columns']:
        actual_fields = set(col.keys())
        missing_fields = expected_fields - actual_fields
        assert not missing_fields, f"Column '{col['name']}' missing fields: {missing_fields}"
    
    print(f"✅ All {len(metadata['row_groups'][0]['columns'])} columns have complete metadata")


if __name__ == "__main__":
    pytest.main([__file__])
