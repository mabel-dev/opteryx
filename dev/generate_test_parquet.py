#!/usr/bin/env python3
"""
Test Parquet file generator for rugo testing.

This script creates Parquet files with configurable:
- Column types and data
- Encoding schemes  
- Compression codecs
- Row group sizes
- Number of row groups

Useful for testing the rugo decoder with various Parquet configurations.
"""

import os
import random
import string
import sys
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

# Add current directory to path for running from repo
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise ImportError("This script requires PyArrow. Install with: pip install pyarrow") from exc


def generate_test_data(
    column_specs: List[Dict[str, Any]], 
    num_rows: int, 
    seed: Optional[int] = None
) -> Dict[str, List[Any]]:
    """
    Generate test data based on column specifications.
    
    Args:
        column_specs: List of column specifications, each containing:
            - name: str - Column name
            - type: str - Data type ('int32', 'int64', 'string', 'float32', 'float64', 'bool', 'date', 'timestamp')
            - nullable: bool - Whether column can contain nulls (default False)
            - null_rate: float - Fraction of values that should be null (default 0.0)
            - pattern: str - Pattern for string generation ('random', 'sequential', 'repeated')
            - min_value: Any - Minimum value for numeric types
            - max_value: Any - Maximum value for numeric types
            - string_length: int - Length for random strings (default 10)
        num_rows: Number of rows to generate
        seed: Random seed for reproducible data
        
    Returns:
        Dictionary mapping column names to lists of values
    """
    if seed is not None:
        random.seed(seed)
    
    data = {}
    
    for spec in column_specs:
        name = spec['name']
        col_type = spec['type']
        nullable = spec.get('nullable', False)
        null_rate = spec.get('null_rate', 0.0)
        
        values = []
        
        for i in range(num_rows):
            # Generate null values based on null_rate
            if nullable and random.random() < null_rate:
                values.append(None)
                continue
                
            # Generate values based on type
            if col_type == 'int32':
                min_val = spec.get('min_value', 0)
                max_val = spec.get('max_value', 2**31 - 1)
                values.append(random.randint(min_val, max_val))
                
            elif col_type == 'int64':
                min_val = spec.get('min_value', 0)
                max_val = spec.get('max_value', 2**63 - 1)
                values.append(random.randint(min_val, max_val))
                
            elif col_type in ('float32', 'float64'):
                min_val = spec.get('min_value', 0.0)
                max_val = spec.get('max_value', 1000.0)
                values.append(random.uniform(min_val, max_val))
                
            elif col_type == 'bool':
                values.append(random.choice([True, False]))
                
            elif col_type == 'string':
                pattern = spec.get('pattern', 'random')
                string_length = spec.get('string_length', 10)
                
                if pattern == 'sequential':
                    values.append(f"string_{i:06d}")
                elif pattern == 'repeated':
                    # Create repeated patterns for better compression
                    values.append(f"value_{i % 100}")
                else:  # random
                    chars = string.ascii_letters + string.digits
                    values.append(''.join([random.choice(chars) for _ in range(string_length)]))
                    
            elif col_type == 'date':
                start_date = datetime(2020, 1, 1).date()
                days_offset = random.randint(0, 365 * 3)  # 3 years range
                values.append(start_date + timedelta(days=days_offset))
                
            elif col_type == 'timestamp':
                start_time = datetime(2020, 1, 1)
                seconds_offset = random.randint(0, 365 * 24 * 3600 * 3)  # 3 years range
                values.append(start_time + timedelta(seconds=seconds_offset))
                
            else:
                raise ValueError(f"Unsupported column type: {col_type}")
        
        data[name] = values
    
    return data


def create_arrow_table(
    data: Dict[str, List[Any]], 
    column_specs: List[Dict[str, Any]]
) -> pa.Table:
    """
    Create a PyArrow table from the generated data with appropriate types.
    
    Args:
        data: Dictionary of column data
        column_specs: Column specifications for type mapping
        
    Returns:
        PyArrow Table
    """
    arrays = []
    names = []
    
    # Create type mapping
    type_map = {spec['name']: spec for spec in column_specs}
    
    for name, values in data.items():
        spec = type_map[name]
        col_type = spec['type']
        
        # Map to PyArrow types
        if col_type == 'int32':
            pa_type = pa.int32()
        elif col_type == 'int64':
            pa_type = pa.int64()
        elif col_type == 'float32':
            pa_type = pa.float32()
        elif col_type == 'float64':
            pa_type = pa.float64()
        elif col_type == 'bool':
            pa_type = pa.bool_()
        elif col_type == 'string':
            pa_type = pa.string()
        elif col_type == 'date':
            pa_type = pa.date32()
        elif col_type == 'timestamp':
            pa_type = pa.timestamp('us')
        else:
            raise ValueError(f"Unsupported column type: {col_type}")
        
        # Create array
        array = pa.array(values, type=pa_type)
        arrays.append(array)
        names.append(name)
    
    return pa.table(arrays, names=names)


def create_test_parquet_file(
    output_path: str,
    column_specs: List[Dict[str, Any]],
    rows_per_group: int = 1000,
    num_groups: int = 3,
    compression: str = 'snappy',
    encoding: Optional[Dict[str, str]] = None,
    seed: Optional[int] = 42
) -> Dict[str, Any]:
    """
    Create a test Parquet file with specified configuration.
    
    Args:
        output_path: Path where the Parquet file will be saved
        column_specs: List of column specifications (see generate_test_data for format)
        rows_per_group: Number of rows per row group
        num_groups: Number of row groups to create
        compression: Compression codec ('none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd')
        encoding: Optional dictionary mapping column names to encoding types
                 ('PLAIN', 'DICTIONARY', 'DELTA_BINARY_PACKED', 'DELTA_LENGTH_BYTE_ARRAY', etc.)
        seed: Random seed for reproducible data
        
    Returns:
        Dictionary with file statistics and metadata
    """
    total_rows = rows_per_group * num_groups
    
    print(f"Creating Parquet file: {output_path}")
    print(f"  Columns: {len(column_specs)}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Row groups: {num_groups}")
    print(f"  Rows per group: {rows_per_group:,}")
    print(f"  Compression: {compression}")
    
    # Generate all data at once
    data = generate_test_data(column_specs, total_rows, seed)
    
    # Create PyArrow table
    table = create_arrow_table(data, column_specs)
    
    # Set up encoding if specified
    column_encodings = {}
    if encoding:
        for col_name, enc_type in encoding.items():
            if enc_type.upper() == 'PLAIN':
                column_encodings[col_name] = 'PLAIN'
            elif enc_type.upper() == 'DICTIONARY':
                column_encodings[col_name] = 'RLE_DICTIONARY'
            else:
                column_encodings[col_name] = enc_type.upper()
    
    # Write the file with row group control
    # Use write_table with row_group_size instead of ParquetWriter constructor
    if num_groups == 1:
        # Simple case: write entire table at once with specified row group size
        pq.write_table(
            table,
            output_path,
            compression=compression,
            use_dictionary=encoding is not None and any(enc.upper() == 'DICTIONARY' for enc in encoding.values()),
            row_group_size=rows_per_group
        )
    else:
        # Multiple row groups: use ParquetWriter to write in chunks
        with pq.ParquetWriter(
            output_path, 
            table.schema, 
            compression=compression,
            use_dictionary=encoding is not None and any(enc.upper() == 'DICTIONARY' for enc in encoding.values())
        ) as writer:
            
            # Write data in chunks to create multiple row groups
            for group_idx in range(num_groups):
                start_idx = group_idx * rows_per_group
                
                # Create slice of the table for this row group
                group_table = table.slice(start_idx, rows_per_group)
                writer.write_table(group_table)
    
    # Read back file info
    file_info = pq.ParquetFile(output_path)
    file_size = os.path.getsize(output_path)
    
    stats = {
        'file_path': output_path,
        'file_size_bytes': file_size,
        'total_rows': total_rows,
        'num_columns': len(column_specs),
        'num_row_groups': file_info.num_row_groups,
        'compression': compression,
        'column_specs': column_specs,
        'encoding': encoding or {},
        'schema': str(table.schema)
    }
    
    print(f"  File size: {file_size:,} bytes")
    print(f"  Actual row groups: {file_info.num_row_groups}")
    print(f"✅ Created: {output_path}")
    
    return stats


# Example configurations for common test scenarios
def get_rugo_compatible_config() -> List[Dict[str, Any]]:
    """Configuration that should work with rugo's decoder."""
    return [
        {'name': 'id', 'type': 'int32', 'min_value': 1, 'max_value': 1000000},
        {'name': 'value', 'type': 'int64', 'min_value': 0, 'max_value': 2**50},
        {'name': 'name', 'type': 'string', 'pattern': 'sequential', 'string_length': 15},
        {'name': 'category', 'type': 'string', 'pattern': 'repeated', 'string_length': 8},
        {'name': 'is_active', 'type': 'bool'},
        {'name': 'score', 'type': 'float32', 'min_value': 0.0, 'max_value': 100.0},
        {'name': 'rating', 'type': 'float64', 'min_value': 0.0, 'max_value': 1000.0},
    ]


def get_mixed_types_config() -> List[Dict[str, Any]]:
    """Configuration with various data types."""
    return [
        {'name': 'int32_col', 'type': 'int32', 'min_value': -1000, 'max_value': 1000},
        {'name': 'int64_col', 'type': 'int64', 'min_value': 0, 'max_value': 2**40},
        {'name': 'float32_col', 'type': 'float32', 'min_value': -100.0, 'max_value': 100.0},
        {'name': 'float64_col', 'type': 'float64', 'min_value': 0.0, 'max_value': 1000.0},
        {'name': 'bool_col', 'type': 'bool'},
        {'name': 'string_col', 'type': 'string', 'pattern': 'random', 'string_length': 20},
        {'name': 'date_col', 'type': 'date'},
        {'name': 'timestamp_col', 'type': 'timestamp'},
    ]


def get_nullable_config() -> List[Dict[str, Any]]:
    """Configuration with nullable columns."""
    return [
        {'name': 'required_id', 'type': 'int32', 'nullable': False},
        {'name': 'optional_value', 'type': 'int64', 'nullable': True, 'null_rate': 0.1},
        {'name': 'optional_name', 'type': 'string', 'nullable': True, 'null_rate': 0.05, 'pattern': 'random'},
    ]


if __name__ == "__main__":
    # Example usage
    print("Test Parquet File Generator")
    print("=" * 40)
    
    # Create test files directory
    test_dir = "tests/data"
    os.makedirs(test_dir, exist_ok=True)
    
    # Example 1: Basic rugo-compatible file (uncompressed, PLAIN encoding)
    print("\n1. Creating rugo-compatible file...")
    stats1 = create_test_parquet_file(
        output_path=os.path.join(test_dir, "rugo_compatible.parquet"),
        column_specs=get_rugo_compatible_config(),
        rows_per_group=1000,
        num_groups=3,
        compression='none',  # Uncompressed
        encoding={'id': 'PLAIN', 'value': 'PLAIN', 'name': 'PLAIN', 'category': 'PLAIN', 'is_active': 'PLAIN', 'score': 'PLAIN', 'rating': 'PLAIN'},
        seed=42
    )
    
    # Example 2: Compressed file (won't work with rugo)
    print("\n2. Creating compressed file...")
    stats2 = create_test_parquet_file(
        output_path=os.path.join(test_dir, "compressed.parquet"),
        column_specs=get_rugo_compatible_config(),
        rows_per_group=2000,
        num_groups=2,
        compression='snappy',
        seed=42
    )
    
    # Example 3: Mixed types
    print("\n3. Creating mixed types file...")
    stats3 = create_test_parquet_file(
        output_path=os.path.join(test_dir, "mixed_types.parquet"),
        column_specs=get_mixed_types_config(),
        rows_per_group=500,
        num_groups=4,
        compression='none',
        seed=42
    )
    
    # Example 4: Large file for performance testing
    print("\n4. Creating large file...")
    stats4 = create_test_parquet_file(
        output_path=os.path.join(test_dir, "large_test.parquet"),
        column_specs=[
            {'name': 'id', 'type': 'int64'},
            {'name': 'data', 'type': 'string', 'pattern': 'repeated', 'string_length': 50},
            {'name': 'value', 'type': 'float64', 'min_value': 0, 'max_value': 1000},
        ],
        rows_per_group=10000,
        num_groups=5,
        compression='none',
        seed=42
    )
    
    # Example 5: SNAPPY compressed file (supported by rugo)
    print("\n5. Creating SNAPPY compressed file...")
    stats5 = create_test_parquet_file(
        output_path=os.path.join(test_dir, "snappy_compressed.parquet"),
        column_specs=[
            {'name': 'id', 'type': 'int32', 'min_value': 1, 'max_value': 1000},
            {'name': 'name', 'type': 'string', 'pattern': 'sequential', 'string_length': 15},
            {'name': 'value', 'type': 'int64', 'min_value': 0, 'max_value': 1000000},
            {'name': 'score', 'type': 'float64', 'min_value': 0.0, 'max_value': 100.0},
        ],
        rows_per_group=500,
        num_groups=2,
        compression='snappy',  # SNAPPY compression
        seed=42
    )
    
    # Example 6: Dictionary encoded file (for testing dictionary support)
    print("\n6. Creating dictionary encoded file...")
    stats6 = create_test_parquet_file(
        output_path=os.path.join(test_dir, "dictionary_encoded.parquet"),
        column_specs=[
            {'name': 'id', 'type': 'int32', 'min_value': 1, 'max_value': 1000},
            {'name': 'category', 'type': 'string', 'pattern': 'repeated', 'string_length': 10},  # Repeated for dictionary
            {'name': 'status', 'type': 'string', 'pattern': 'repeated', 'string_length': 8},     # Repeated for dictionary
            {'name': 'value', 'type': 'int64', 'min_value': 0, 'max_value': 100000},
        ],
        rows_per_group=500,
        num_groups=2,
        compression='snappy',  # SNAPPY compression
        encoding={'category': 'DICTIONARY', 'status': 'DICTIONARY'},  # Use dictionary encoding
        seed=42
    )
    
    print(f"\n✅ All test files created in: {test_dir}/")
    print("\nTest with rugo:")
    print("  import rugo.parquet as rp")
    print(f"  rp.can_decode('{os.path.join(test_dir, 'snappy_compressed.parquet')}')")
    print(f"  rp.can_decode('{os.path.join(test_dir, 'dictionary_encoded.parquet')}')")
