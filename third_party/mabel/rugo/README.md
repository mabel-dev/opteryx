# rugo

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/rugo?period=total&units=INTERNATIONAL_SYSTEM&left_color=BRIGHTGREEN&right_color=LIGHTGREY&left_text=downloads)](https://pepy.tech/projects/rugo)

`rugo` is a C++17 and Cython powered file reader for Python. It delivers high-throughput reading for both Parquet files (metadata inspection and experimental column reader) and JSON Lines files (with schema inference, projection pushdown, and SIMD optimizations). **JSON Lines reader returns high-performance draken vectors** for efficient columnar processing. The data-reading API is evolving rapidly and will change in upcoming releases.

## Key Features
- **Parquet**: Fast metadata extraction backed by an optimized C++17 parser and thin Python bindings.
- **Parquet**: Complete schema and row-group details, including encodings, codecs, offsets, bloom filter pointers, and custom key/value metadata.
- **Parquet**: Experimental memory-based data reading for PLAIN and RLE_DICTIONARY encoded columns with UNCOMPRESSED, SNAPPY, and ZSTD codecs.
- **JSON Lines**: High-performance columnar reader with schema inference, projection pushdown, and SIMD optimizations (19% faster).
- **JSON Lines**: Returns draken vectors with zero-copy Arrow interoperability for high-performance columnar processing.
- **JSON Lines**: Memory-based processing for zero-copy parsing.
- Works with file paths, byte strings, and contiguous memoryviews.
- Optional schema conversion helpers for [Orso](https://github.com/mabel-dev/orso).
- Runtime dependencies: [draken](https://github.com/mabel-dev/draken) (for JSON Lines reader), [pyarrow](https://arrow.apache.org/docs/python/) (for Arrow interoperability).

## Installation

### PyPI
```bash
pip install rugo

# Optional extras
pip install rugo[orso]
pip install rugo[dev]
```

### From source
```bash
git clone https://github.com/mabel-dev/rugo.git
cd rugo
python -m venv .venv
source .venv/bin/activate
make update
make compile
pip install -e .
```

### Requirements
- Python 3.9 or newer
- A C++17 compatible compiler (clang, gcc, or MSVC)
- Cython and setuptools for source builds (installed by the commands above)
- On x86-64 platforms, an assembler capable of compiling `.S` sources (bundled with modern GCC/Clang toolchains)
- ARM/AArch64 platforms (including Apple Silicon) are fully supported with NEON SIMD optimizations

### Runtime Dependencies
- **draken**: High-performance columnar vector library (for JSON Lines reader)
- **pyarrow**: Apache Arrow Python bindings (for Arrow interoperability)

## Quickstart
```python
import rugo.parquet as parquet_meta

metadata = parquet_meta.read_metadata("example.parquet")

print(f"Rows: {metadata['num_rows']}")
print("Schema columns:")
for column in metadata["schema_columns"]:
    print(f"  {column['name']}: {column['physical_type']} ({column['logical_type']})")

first_row_group = metadata["row_groups"][0]
for column in first_row_group["columns"]:
    print(
        f"{column['name']}: codec={column['compression_codec']}, "
        f"nulls={column['null_count']}, range=({column['min']}, {column['max']})"
    )
```
`read_metadata` returns dictionaries composed of Python primitives, ready for JSON serialisation or downstream processing.

## Returned metadata layout
```python
{
    "num_rows": int,
    "schema_columns": [
        {
            "name": str,
            "physical_type": str,
            "logical_type": str,
            "nullable": bool,
        },
        ...
    ],
    "row_groups": [
        {
            "num_rows": int,
            "total_byte_size": int,
            "columns": [
                {
                    "name": str,
                    "path_in_schema": str,
                    "physical_type": str,
                    "logical_type": str,
                    "num_values": Optional[int],
                    "total_uncompressed_size": Optional[int],
                    "total_compressed_size": Optional[int],
                    "data_page_offset": Optional[int],
                    "index_page_offset": Optional[int],
                    "dictionary_page_offset": Optional[int],
                    "min": Any,
                    "max": Any,
                    "null_count": Optional[int],
                    "distinct_count": Optional[int],
                    "bloom_offset": Optional[int],
                    "bloom_length": Optional[int],
                    "encodings": List[str],
                    "compression_codec": Optional[str],
                    "key_value_metadata": Optional[Dict[str, str]],
                },
                ...
            ],
        },
        ...
    ],
}
```
Fields that are not present in the source Parquet file are reported as `None`. Minimum and maximum values are decoded into Python types when possible; otherwise hexadecimal strings are returned.

## Parsing options
All entry points share the same keyword arguments:

- `schema_only` (default `False`): return only the top-level schema without row group details.
- `include_statistics` (default `True`): skip min/max/num_values decoding when set to `False`.
- `max_row_groups` (default `-1`): limit the number of row groups inspected; handy for very large files.

```python
metadata = parquet_meta.read_metadata(
    "large_file.parquet",
    schema_only=False,
    include_statistics=False,
    max_row_groups=2,
)
```

## Working with in-memory data
```python
with open("example.parquet", "rb") as fh:
    data = fh.read()

from_bytes = parquet_meta.read_metadata_from_bytes(data)
from_view = parquet_meta.read_metadata_from_memoryview(memoryview(data))
```
`read_metadata_from_memoryview` performs zero-copy parsing when given a contiguous buffer.

## Prototype Data Decoding (Experimental)
> **API stability:** The column-reading functions are experimental and will change without notice while we expand format coverage.

`rugo` includes a prototype decoder for reading actual column data from Parquet files. This is a **limited, experimental feature** designed for simple use cases and testing.

### Supported Features
- ✅ UNCOMPRESSED, SNAPPY, and ZSTD codecs
- ✅ PLAIN encoding
- ✅ RLE_DICTIONARY encoding
- ✅ `int32`, `int64`, `float32`, `float64`, `boolean`, and `string` (byte_array) types
- ✅ Memory-based processing (load once, decode multiple times)
- ✅ Column selection (decode only the columns you need)
- ✅ Multi-row-group support

### Unsupported Features  
- ❌ Other codecs (GZIP, LZ4, LZO, BROTLI, etc.)
- ❌ Delta encoding, PLAIN_DICTIONARY, other advanced encodings
- ❌ Nullable columns with definition levels > 0
- ❌ Other types (int96, fixed_len_byte_array, date, timestamp, complex types)
- ❌ Nested structures (lists, maps, structs)

### Primary API: Memory-Based Reading

The recommended approach loads Parquet data into memory once and performs all operations on the in-memory buffer:

```python
import rugo.parquet as rp

# Load file into memory once
with open("data.parquet", "rb") as f:
    parquet_data = f.read()

# Check if the data can be decoded
if rp.can_decode_from_memory(parquet_data):
    
    # Read ALL columns from all row groups
    table = rp.read_parquet(parquet_data)
    
    # Or read SPECIFIC columns only
    table = rp.read_parquet(parquet_data, ["name", "age", "salary"])
    
    # Access the structured data
    print(f"Columns: {table['column_names']}")
    print(f"Row groups: {len(table['row_groups'])}")
    
    # Iterate through row groups and columns
    for rg_idx, row_group in enumerate(table['row_groups']):
        print(f"Row group {rg_idx}:")
        for col_idx, column_data in enumerate(row_group):
            col_name = table['column_names'][col_idx]
            if column_data is not None:
                print(f"  {col_name}: {len(column_data)} values")
            else:
                print(f"  {col_name}: Failed to decode")
```

### Data Structure
The `read_parquet()` function returns a dictionary with this structure:
```python
{
    'success': bool,                    # True if reading succeeded
    'column_names': ['col1', 'col2'],   # List of column names
    'row_groups': [                     # List of row groups
        [col1_data, col2_data],         # Row group 0: list of columns
        [col1_data, col2_data],         # Row group 1: list of columns
        # ... more row groups
    ]
}
```
Each column's data is a Python list containing the decoded values.

### Performance Benefits

**Traditional Approach (Multiple File I/O):**
```python
# Each operation reads the file separately
metadata = rp.read_metadata("file.parquet")       # File I/O #1
col1 = rp.decode_column("file.parquet", "col1")   # File I/O #2  
col2 = rp.decode_column("file.parquet", "col2")   # File I/O #3
```

**Memory-Based Approach (Single File I/O):**
```python
# Load once, process multiple times
with open("file.parquet", "rb") as f:
    data = f.read()  # File I/O #1 (only)

table = rp.read_parquet(data, ["col1", "col2"])   # In-memory processing
```

### Legacy File-Based API
For backward compatibility, file-based functions are still available:

```python
# Check if a file can be decoded
if rp.can_decode("data.parquet"):
    # Decode a specific column from first row group only
    values = rp.decode_column("data.parquet", "column_name")
    print(values)  # e.g., [1, 2, 3, 4, 5] or ['a', 'b', 'c']
```

### Use Cases
The memory-based API is optimized for:
- **Query engines** with metadata-driven pruning
- **ETL pipelines** processing multiple Parquet files
- **Data exploration** where you need to examine various columns
- **High-performance scenarios** minimizing I/O operations

See `examples/memory_based_api_example.py` and `examples/optional_columns_example.py` for complete demonstrations.

**Note:** This decoder is a **prototype** for educational and testing purposes. For production use with full Parquet support, use [PyArrow](https://arrow.apache.org/docs/python/) or [FastParquet](https://github.com/dask/fastparquet).

## JSON Lines Reading

`rugo` includes a high-performance JSON Lines reader with schema inference, projection pushdown, and SIMD optimizations. **Returns data as high-performance draken vectors** for efficient columnar processing.

### Features
- ✅ Fast columnar reading with C++17 implementation and SIMD optimizations
- ✅ **19% performance improvement** from SIMD optimizations (AVX2/SSE2)
- ✅ **Draken vector output** - High-performance columnar vectors with zero-copy Arrow interoperability
- ✅ Automatic schema inference from JSON data
- ✅ Projection pushdown (read only needed columns)
- ✅ Support for int64, double, string, and boolean types
- ✅ Native null value handling
- ✅ Memory-based processing (zero-copy parsing)
- ✅ Orso schema conversion

### Quick Example

```python
import rugo.jsonl as rj

# Sample JSON Lines data
data = b'''{"id": 1, "name": "Alice", "age": 30, "salary": 50000.0}
{"id": 2, "name": "Bob", "age": 25, "salary": 45000.0}
{"id": 3, "name": "Charlie", "age": 35, "salary": 55000.0}'''

# Get schema
schema = rj.get_jsonl_schema(data)
print(f"Columns: {[col['name'] for col in schema]}")
# Output: Columns: ['id', 'name', 'age', 'salary']

# Read all columns - returns draken vectors
result = rj.read_jsonl(data)
print(f"Read {result['num_rows']} rows with {len(result['columns'])} columns")
print(f"Column types: {[type(col).__name__ for col in result['columns']]}")
# Output: Column types: ['Int64Vector', 'StringVector', 'Int64Vector', 'Float64Vector']

# Read with projection (only specific columns)
result = rj.read_jsonl(data, columns=['name', 'salary'])
# Only reads 'name' and 'salary' - projection pushdown!
# Returns StringVector and Float64Vector
```

### Working with Files

```python
import rugo.jsonl as rj

# Load file into memory
with open("data.jsonl", "rb") as f:
    jsonl_data = f.read()

# Extract schema
schema = rj.get_jsonl_schema(jsonl_data, sample_size=1000)

# Read specific columns only - returns draken vectors
result = rj.read_jsonl(jsonl_data, columns=['user_id', 'email', 'score'])

# Access columnar data (vectors support iteration and to_arrow() conversion)
# Convert to Arrow arrays for easy iteration
user_ids = result['columns'][0].to_arrow()
emails = result['columns'][1].to_arrow()
scores = result['columns'][2].to_arrow()

for i in range(result['num_rows']):
    user_id = user_ids[i]
    email = emails[i]
    score = scores[i]
    print(f"User {user_id}: {email} - Score: {score}")
```
```

### Orso Integration

```python
import rugo.jsonl as rj
from rugo.converters.orso import jsonl_to_orso_schema

# Get JSON Lines schema
jsonl_schema = rj.get_jsonl_schema(data)

# Convert to Orso schema
orso_schema = jsonl_to_orso_schema(jsonl_schema, schema_name="my_table")
print(f"Schema: {orso_schema.name}")
for col in orso_schema.columns:
    print(f"  {col.name}: {col.type}")
```

### Performance

The JSON Lines reader achieves approximately **109K-201K rows/second** on wide tables (50 columns), with higher throughput on narrower tables. With SIMD optimizations (AVX2/SSE2), the reader delivers:

- **Full read (50 cols)**: ~109K rows/second
- **Projection (10 cols)**: ~174-191K rows/second
- **Projection (5 cols)**: ~181-201K rows/second
- **Performance improvement**: 19% faster with SIMD optimizations

The SIMD implementation uses:
- **AVX2**: Processes 32 bytes at once for newline detection and text parsing (preferred)
- **SSE2**: Processes 16 bytes at once (fallback)
- **Scalar fallback**: Byte-by-byte processing for non-x86 architectures

#### Draken Vectors Performance Benefits

The JSON Lines reader returns **draken vectors** instead of Python lists, providing:
- **Faster processing**: 2-5x faster for type-specific operations compared to Python lists
- **Lower memory usage**: 20-40% reduction in memory footprint
- **Zero-copy Arrow interop**: Seamless integration with PyArrow and other Arrow-based tools
- **SIMD optimizations**: Automatic vectorization on x86_64 and ARM platforms
- **Type-specialized operations**: Optimized kernels for int64, float64, string, and boolean operations

#### Comparison with Opteryx

On 50-column datasets, rugo is **2.7-5.6x faster** than Opteryx 0.25.1 (release):
- **Full read**: 2.7-3.1x faster
- **Projection (10 cols)**: 3.8-5.4x faster
- **Projection (5 cols)**: 3.9-5.6x faster

**Note**: These benchmarks compare against Opteryx 0.25.1 (PyPI release) which uses a Python-based decoder with csimdjson. The main branch (0.26.0+) includes a new Cython-based fast decoder with SIMD optimizations that is expected to be significantly faster.

rugo's advantages:
- ✅ **True projection pushdown**: Only parse columns you need
- ✅ **Memory-based**: No file I/O overhead
- ✅ **Zero-copy design**: Direct memory-to-column conversion
- ✅ **Consistent performance**: Maintains throughput across dataset sizes

See `PERFORMANCE_COMPARISON.md` for detailed benchmark results, `JSONL_SIMD_OPTIMIZATIONS.md` for SIMD optimization details, and `OPTERYX_DECODER_ANALYSIS.md` for a technical analysis of Opteryx's Cython decoder and potential improvements.

See `examples/read_jsonl.py` and `benchmarks/compare_opteryx_performance.py` for complete demonstrations.

## Optional Orso conversion
Install the optional extra (`pip install rugo[orso]`) to enable Orso helpers:
```python
from rugo.converters.orso import extract_schema_only, rugo_to_orso_schema, jsonl_to_orso_schema

# Parquet to Orso
metadata = parquet_meta.read_metadata("example.parquet")
relation = rugo_to_orso_schema(metadata, "example_table")
schema_info = extract_schema_only(metadata)

# JSON Lines to Orso
import rugo.jsonl as rj
jsonl_schema = rj.get_jsonl_schema(data)
relation = jsonl_to_orso_schema(jsonl_schema, "jsonl_table")
```
See `examples/orso_conversion.py` and `examples/jsonl_orso_conversion.py` for complete walkthroughs.

## Development
```bash
make update     # install build and test tooling (uses uv under the hood)
make compile    # rebuild the Cython extension with -O3 and C++17 flags
make test       # run pytest-based validation (includes PyArrow comparisons)
make lint       # run ruff, isort, pycln, cython-lint
make mypy       # type checking
```
`make compile` clears previous build artefacts before rebuilding the extension in-place.

## Project layout
```
rugo/
├── rugo/__init__.py
├── rugo/parquet/
│   ├── parquet_reader.pyx
│   ├── parquet_reader.pxd
│   ├── parquet_reader.cpp
│   ├── metadata.cpp
│   ├── metadata.hpp
│   ├── bloom_filter.cpp
│   ├── decode.cpp
│   ├── decode.hpp
│   ├── compression.cpp
│   ├── compression.hpp
│   ├── thrift.hpp
│   └── vendor/
├── rugo/jsonl_src/
│   ├── jsonl.pyx
│   ├── jsonl.pxd
│   ├── jsonl_reader.cpp
│   └── jsonl_reader.hpp
├── rugo/converters/orso.py
├── examples/
│   ├── read_parquet_metadata.py
│   ├── read_parquet_data.py
│   ├── read_jsonl.py
│   ├── jsonl_orso_conversion.py
│   ├── create_test_file.py
│   └── orso_conversion.py
├── scripts/
│   ├── generate_test_parquet.py
│   └── vendor_compression_libs.py
├── tests/
│   ├── data/
│   ├── test_all_metadata_fields.py
│   ├── test_bloom_filter.py
│   ├── test_decode.py
│   ├── test_jsonl.py
│   ├── test_jsonl_performance.py
│   ├── test_logical_types.py
│   ├── test_orso_converter.py
│   ├── test_statistics.py
│   └── requirements.txt
├── Makefile
├── pyproject.toml
├── setup.py
└── README.md
```

## Status and limitations
- Active development status (alpha); APIs are evolving and may change between releases.
- **Parquet**: Metadata APIs are largely stable. The column-reading API is experimental and will change.
- **JSON Lines**: High-performance reader with SIMD optimizations (19% improvement) and basic type support (int64, double, string, boolean).
- Requires a C++17 compiler when installing from source or editing the Cython bindings.
- SIMD optimizations (AVX2/SSE2) are automatically enabled on x86-64 platforms.
- Bloom filter information is exposed via offsets and lengths; higher-level helpers are planned.

## Future Format Support
rugo currently supports Parquet and JSON Lines. We are evaluating additional formats based on optimization opportunities and community demand. See [FORMAT_SUPPORT_ANALYSIS.md](docs/FORMAT_SUPPORT_ANALYSIS.md) for a detailed analysis of candidate formats (ORC, Avro, CSV, Arrow IPC) and [FORMAT_ROADMAP.md](docs/FORMAT_ROADMAP.md) for our planned roadmap. Feedback and feature requests are welcome!

## License
Licensed under the Apache License 2.0. See `LICENSE` for full terms.

## Maintainer
Created and maintained by Justin Joyce (`@joocer`). Contributions are welcome via issues and pull requests.
