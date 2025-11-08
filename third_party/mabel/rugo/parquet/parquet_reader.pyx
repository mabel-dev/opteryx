# distutils: language = c++
# distutils: extra_compile_args = -Wno-unreachable-code-fallthrough
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

import datetime
import os
import struct

cimport parquet_reader
from libc.stdint cimport uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector


# --- value decoder ---
cdef inline bint _text_is_printable(str text):
    for ch in text:
        code = ord(ch)
        if code < 32 and ch not in ('\t', '\n', '\r'):
            return False
        if code == 127:
            return False
    return True


cdef inline str _safe_decode_utf8(string raw_bytes):
    """Safely decode bytes to UTF-8 string, handling invalid sequences."""
    cdef bytes b = raw_bytes
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        # Fall back to latin-1 (which can decode any byte sequence)
        # or use error handling to replace invalid characters
        try:
            return b.decode("utf-8", errors="replace")
        except Exception:
            # Ultimate fallback: decode as latin-1 which never fails
            return b.decode("latin-1")


cdef object decode_value(
        string physical_type,
        string logical_type,
        string raw,
        bint prefer_text):
    cdef bytes b = raw
    if b is None:
        return None

    # Decode the C++ string to Python string for comparison
    cdef str type_str = physical_type.decode("utf-8")
    cdef str logical_str = logical_type.decode("utf-8") if logical_type.size() > 0 else ""
    cdef bint is_string_logical = (
        logical_str in ("varchar", "UTF8", "JSON", "BSON", "ENUM")
        or logical_str.startswith("array<string")
        or logical_str.startswith("array<varchar")
    )
    cdef object candidate

    if len(b) == 0:
        if type_str in ("byte_array", "fixed_len_byte_array"):
            if is_string_logical or prefer_text:
                return ""
        return b""   # treat empty binary as bytes for non-string types

    try:
        if type_str == "int32":
            return struct.unpack("<i", b)[0]
        elif type_str == "int64":
            return struct.unpack("<q", b)[0]
        elif type_str == "float32":
            return struct.unpack("<f", b)[0]
        elif type_str == "float64":
            return struct.unpack("<d", b)[0]
        elif type_str in ("byte_array", "fixed_len_byte_array"):
            # If logical type indicates UTF-8 string, decode it
            # Handle "varchar" (new format) and legacy "UTF8" format
            # Also handle array<string> and array<varchar> - the elements are UTF-8 strings
            if is_string_logical:
                try:
                    return b.decode("utf-8")
                except UnicodeDecodeError:
                    # If UTF-8 decoding fails, return as bytes
                    return b
            elif prefer_text and type_str == "byte_array":
                try:
                    candidate = b.decode("utf-8")
                except UnicodeDecodeError:
                    return b
                if _text_is_printable(candidate) and "\ufffd" not in candidate:
                    return candidate
            # Otherwise, return raw bytes (binary data)
            return b
        elif type_str == "int96":
            if len(b) == 12:
                lo, hi = struct.unpack("<qI", b)
                julian_day = hi
                nanos = lo
                # convert Julian day
                days = julian_day - 2440588
                date = datetime.date(1970, 1, 1) + datetime.timedelta(days=days)
                seconds = nanos // 1_000_000_000
                micros = (nanos % 1_000_000_000) // 1000
                return f"{date.isoformat()} {seconds:02d}:{(micros/1e6):.6f}"
            return b.hex()
        elif type_str == "boolean":
            # Parquet encodes boolean as 1 bit, usually in a byte
            return b[0] != 0
        else:
            return b.hex()
    except Exception:
        return b.hex()


cdef parquet_reader.MetadataParseOptions _build_options(
        bint schema_only,
        bint include_statistics,
        Py_ssize_t max_row_groups):
    cdef parquet_reader.MetadataParseOptions opts = parquet_reader.MetadataParseOptions()
    opts.schema_only = schema_only
    if schema_only:
        opts.include_statistics = False
    else:
        opts.include_statistics = include_statistics
    if max_row_groups >= 0:
        opts.max_row_groups = <long long>max_row_groups
    else:
        opts.max_row_groups = -1
    return opts


cdef object _filestats_to_python(parquet_reader.FileStats fs,
                                 bint include_row_groups):
    cdef dict result = {"num_rows": fs.num_rows}

    cdef list schema_columns = []
    cdef parquet_reader.SchemaField field
    cdef size_t idx
    cdef dict top_level_types = {}
    for idx in range(fs.schema_columns.size()):
        field = fs.schema_columns[idx]
        field_name = field.name.decode("utf-8")
        field_physical = field.physical_type.decode("utf-8")
        field_logical = field.logical_type.decode("utf-8")
        schema_columns.append({
            "name": field_name,
            "physical_type": field_physical,
            "logical_type": field_logical,
            "nullable": bool(field.nullable),
        })
        top_level_types[field_name] = {
            "logical": field_logical,
            "physical": field_physical,
        }
    result["schema_columns"] = schema_columns

    if include_row_groups and fs.row_groups.size() > 0:
        row_groups = []
        for rg in fs.row_groups:
            rg_dict = {
                "num_rows": rg.num_rows,
                "total_byte_size": rg.total_byte_size,
                "columns": []
            }
            for col in rg.columns:
                physical_type_str = col.physical_type.decode("utf-8")
                if col.logical_type.size() > 0:
                    logical_type_str = col.logical_type.decode("utf-8")
                else:
                    logical_type_str = ""

                full_name = col.name.decode("utf-8")
                if "." in full_name:
                    display_name = full_name.split(".", 1)[0]
                else:
                    display_name = full_name

                top_level_info = top_level_types.get(display_name)
                if top_level_info is not None:
                    top_level_type = top_level_info.get("logical", "")
                    prefer_text = top_level_type == "json" or top_level_type.startswith("array<")
                else:
                    top_level_type = ""
                    prefer_text = False

                if full_name != display_name and top_level_info is not None:
                    logical_type_str = top_level_info.get("logical", logical_type_str)

                null_count = col.null_count if col.null_count >= 0 else None
                distinct_count = col.distinct_count if col.distinct_count >= 0 else None
                num_values = col.num_values if col.num_values >= 0 else None
                total_uncompressed_size = col.total_uncompressed_size if col.total_uncompressed_size >= 0 else None
                total_compressed_size = col.total_compressed_size if col.total_compressed_size >= 0 else None
                data_page_offset = col.data_page_offset if col.data_page_offset >= 0 else None
                index_page_offset = col.index_page_offset if col.index_page_offset >= 0 else None
                dictionary_page_offset = col.dictionary_page_offset if col.dictionary_page_offset >= 0 else None
                bloom_offset = col.bloom_offset if col.bloom_offset >= 0 else None
                bloom_length = col.bloom_length if col.bloom_length >= 0 else None

                min_val = decode_value(
                    col.physical_type,
                    col.logical_type,
                    col.min,
                    prefer_text) if col.has_min else None
                max_val = decode_value(
                    col.physical_type,
                    col.logical_type,
                    col.max,
                    prefer_text) if col.has_max else None

                encodings_list = []
                for enc in col.encodings:
                    encodings_list.append(parquet_reader.EncodingToString(enc).decode("utf-8"))

                codec_str = None
                if col.codec >= 0:
                    codec_str = parquet_reader.CompressionCodecToString(col.codec).decode("utf-8")

                kv_metadata = {}
                for item in col.key_value_metadata:
                    kv_metadata[item.first.decode("utf-8")] = item.second.decode("utf-8")

                rg_dict["columns"].append({
                    "name": display_name,
                    "path_in_schema": full_name,
                    "physical_type": physical_type_str,
                    "logical_type": logical_type_str,
                    "min": min_val,
                    "max": max_val,
                    "null_count": null_count,
                    "distinct_count": distinct_count,
                    "num_values": num_values,
                    "total_uncompressed_size": total_uncompressed_size,
                    "total_compressed_size": total_compressed_size,
                    "data_page_offset": data_page_offset,
                    "index_page_offset": index_page_offset,
                    "dictionary_page_offset": dictionary_page_offset,
                    "bloom_offset": bloom_offset,
                    "bloom_length": bloom_length,
                    "encodings": encodings_list,
                    "compression_codec": codec_str,
                    "key_value_metadata": kv_metadata if kv_metadata else None,
                })
            row_groups.append(rg_dict)
        result["row_groups"] = row_groups
    else:
        result["row_groups"] = []

    return result


def read_metadata(str path, *, bint schema_only=False,
                  bint include_statistics=True, Py_ssize_t max_row_groups=-1):
    """Read parquet metadata from a file path."""
    cdef parquet_reader.MetadataParseOptions opts = _build_options(
        schema_only, include_statistics, max_row_groups
    )
    cdef bytes path_bytes = path.encode("utf-8")
    cdef const char* c_path = path_bytes
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataC(
        c_path, opts
    )
    return _filestats_to_python(fs, not schema_only)


def read_metadata_from_bytes(bytes data, *, bint schema_only=False,
                             bint include_statistics=True,
                             Py_ssize_t max_row_groups=-1):
    """Read parquet metadata from an in-memory bytes object."""
    cdef parquet_reader.MetadataParseOptions opts = _build_options(
        schema_only, include_statistics, max_row_groups
    )
    cdef const uint8_t* buf = <const uint8_t*> data
    cdef size_t size = len(data)
    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return _filestats_to_python(fs, not schema_only)


def read_metadata_from_memoryview(memoryview mv, *, bint schema_only=False,
                                  bint include_statistics=True,
                                  Py_ssize_t max_row_groups=-1):
    """Read parquet metadata from a Python memoryview (zero-copy)."""
    if not mv.contiguous:
        raise ValueError("Memoryview must be contiguous")

    cdef parquet_reader.MetadataParseOptions opts = _build_options(
        schema_only, include_statistics, max_row_groups
    )
    cdef memoryview[uint8_t] mv_bytes = mv.cast('B')  # keep reference alive
    cdef const uint8_t* buf = &mv_bytes[0]
    cdef size_t size = mv_bytes.nbytes

    cdef parquet_reader.FileStats fs = parquet_reader.ReadParquetMetadataFromBuffer(
        buf, size, opts
    )
    return _filestats_to_python(fs, not schema_only)


def can_decode(str path):
    """Check if a parquet file can be decoded with our limited decoder.

    Returns True only if:
    - All columns are uncompressed
    - All columns use PLAIN encoding
    - All columns are int32, int64, or string types
    """
    cdef bytes path_bytes = path.encode("utf-8")
    cdef string cpp_path = path_bytes
    return parquet_reader.CanDecode(cpp_path)

def test_bloom_filter(path, bloom_offset, bloom_length, value):
    """Evaluate a parquet column bloom filter at the given offset."""
    if bloom_offset is None:
        raise ValueError("Bloom filter offset is required")

    cdef long long native_offset = <long long>bloom_offset
    if native_offset < 0:
        raise ValueError("Bloom filter offset must be non-negative")

    cdef long long native_length
    if bloom_length is None:
        native_length = -1
    else:
        native_length = <long long>bloom_length
        if native_length <= 0:
            native_length = -1

    cdef str path_str = os.fspath(path)
    cdef bytes path_bytes = path_str.encode("utf-8")

    if isinstance(value, (bytes, bytearray, memoryview)):
        value_bytes = bytes(value)
    else:
        value_bytes = str(value).encode("utf-8")

    cdef parquet_reader.string c_path = path_bytes
    cdef parquet_reader.string c_value = value_bytes

    return bool(parquet_reader.TestBloomFilter(c_path, native_offset, native_length, c_value))


def can_decode_from_memory(data):
    """Check if parquet data in memory can be decoded with our limited decoder.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data

    Returns:
        bool: True if the data can be decoded, False otherwise
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size

    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]
    return bool(parquet_reader.CanDecode(&mem_view[0], size))


def read_parquet(data, column_names=None):
    """Read parquet data from memory with optional column selection.

    This is the primary API for reading parquet data. It returns a structure
    organized as [row_group][column] containing the decoded values.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data
        column_names: list of column names to read, or None to read all columns

    Returns:
        dict with:
            'success': bool indicating if read was successful
            'column_names': list of column names (same as input, or all columns if None)
            'row_groups': list of row groups, each containing list of columns
                         Each column is a list of decoded values

        Returns None if reading failed.
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size
    cdef vector[string] cpp_column_names

    # Convert input data to memory view
    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    # Call the appropriate C++ function based on whether columns are specified
    cdef parquet_reader.DecodedTable result

    if column_names is None:
        # Decode all columns
        result = parquet_reader.ReadParquet(&mem_view[0], size)
    else:
        # Decode specified columns
        for name in column_names:
            cpp_column_names.push_back(str(name).encode("utf-8"))
        result = parquet_reader.ReadParquet(&mem_view[0], size, cpp_column_names)

    if not result.success:
        return None

    # Convert result to Python structure
    python_result = {
        'success': True,
        'column_names': [name.decode("utf-8") for name in result.column_names],
        'row_groups': []
    }

    # Convert each row group
    for rg_idx in range(result.row_groups.size()):
        row_group = []
        for col_idx in range(result.row_groups[rg_idx].size()):
            column = result.row_groups[rg_idx][col_idx]

            if not column.success:
                row_group.append(None)
                continue

            col_type = column.type.decode("utf-8")

            if col_type == "int32":
                row_group.append(list(column.int32_values))
            elif col_type == "int64":
                row_group.append(list(column.int64_values))
            elif col_type == "byte_array":
                row_group.append([_safe_decode_utf8(s) for s in column.string_values])
            elif col_type == "boolean":
                row_group.append([bool(val) for val in column.boolean_values])
            elif col_type == "float32":
                row_group.append(list(column.float32_values))
            elif col_type == "float64":
                row_group.append(list(column.float64_values))
            else:
                row_group.append(None)

        python_result['row_groups'].append(row_group)

    return python_result


def decode_column_from_memory(data, str column_name, row_group_stats, int row_group_index):
    """Decode a specific column from memory for a specific row group.

    Args:
        data: bytes, bytearray, or memoryview containing parquet data
        column_name: Name of the column to decode
        row_group_stats: RowGroupStats object containing metadata for the row group
        row_group_index: Index of the row group (for reference/debugging)

    Returns a Python list containing the decoded values.
    Only works for uncompressed, PLAIN-encoded int32, int64, string, boolean, float32, and float64 columns.

    Returns None if the column cannot be decoded.
    """
    cdef const uint8_t[::1] mem_view
    cdef size_t size
    cdef parquet_reader.RowGroupStats cpp_row_group
    cdef parquet_reader.ColumnStats cpp_col

    # Convert input data to memory view
    if isinstance(data, (bytes, bytearray)):
        mem_view = memoryview(data).cast('B')
    elif isinstance(data, memoryview):
        mem_view = data.cast('B')
    else:
        raise TypeError("data must be bytes, bytearray, or memoryview")

    size = mem_view.shape[0]

    # Convert column name
    cdef bytes column_bytes = column_name.encode("utf-8")
    cdef string cpp_column = column_bytes

    # Convert the Python row_group_stats to C++ RowGroupStats
    cpp_row_group.num_rows = row_group_stats.num_rows
    cpp_row_group.total_byte_size = row_group_stats.total_byte_size

    # Convert the columns
    for col in row_group_stats.columns:
        cpp_col.name = col.name.encode("utf-8")
        cpp_col.physical_type = col.physical_type.encode("utf-8")
        cpp_col.logical_type = col.logical_type.encode("utf-8") if col.logical_type else b""
        cpp_col.num_values = col.num_values if col.num_values is not None else -1
        cpp_col.total_uncompressed_size = col.total_uncompressed_size if col.total_uncompressed_size is not None else -1
        cpp_col.total_compressed_size = col.total_compressed_size if col.total_compressed_size is not None else -1
        cpp_col.data_page_offset = col.data_page_offset if col.data_page_offset is not None else -1
        cpp_col.index_page_offset = col.index_page_offset if col.index_page_offset is not None else -1
        cpp_col.dictionary_page_offset = col.dictionary_page_offset if col.dictionary_page_offset is not None else -1
        cpp_col.has_min = col.has_min if col.has_min is not None else False
        cpp_col.has_max = col.has_max if col.has_max is not None else False
        
        # Handle min/max values which can be different types
        if col.min:
            if isinstance(col.min, bytes):
                cpp_col.min = col.min
            elif isinstance(col.min, str):
                cpp_col.min = col.min.encode("utf-8")
            else:
                cpp_col.min = str(col.min).encode("utf-8")
        else:
            cpp_col.min = b""
            
        if col.max:
            if isinstance(col.max, bytes):
                cpp_col.max = col.max
            elif isinstance(col.max, str):
                cpp_col.max = col.max.encode("utf-8")
            else:
                cpp_col.max = str(col.max).encode("utf-8")
        else:
            cpp_col.max = b""
            
        cpp_col.null_count = col.null_count if col.null_count is not None else -1
        cpp_col.distinct_count = col.distinct_count if col.distinct_count is not None else -1
        cpp_col.bloom_offset = col.bloom_offset if col.bloom_offset is not None else -1
        cpp_col.bloom_length = col.bloom_length if col.bloom_length is not None else -1
        cpp_col.encodings = col.encodings if col.encodings is not None else []
        cpp_col.codec = col.codec if col.codec is not None else -1
        cpp_row_group.columns.push_back(cpp_col)

    cdef parquet_reader.DecodedColumn result = parquet_reader.DecodeColumnFromMemory(
        &mem_view[0], size, cpp_column, cpp_row_group, row_group_index)

    if not result.success:
        return None

    cdef str col_type = result.type.decode("utf-8")

    if col_type == "int32":
        return list(result.int32_values)
    elif col_type == "int64":
        return list(result.int64_values)
    elif col_type == "byte_array":
        return [_safe_decode_utf8(s) for s in result.string_values]
    elif col_type == "boolean":
        return [bool(val) for val in result.boolean_values]
    elif col_type == "float32":
        return list(result.float32_values)
    elif col_type == "float64":
        return list(result.float64_values)
    else:
        return None
