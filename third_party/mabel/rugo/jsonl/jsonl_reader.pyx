"""Cython wrapper for reading JSONL files."""

# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

from libc.stdint cimport uint8_t, int64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from cpython.buffer cimport PyBUF_CONTIG_RO, PyObject_GetBuffer, PyBuffer_Release, Py_buffer
import json
from cpython.ref cimport PyObject, Py_INCREF, Py_DECREF
from cpython.exc cimport PyErr_Occurred
from cpython.list cimport PyList_Append
from cpython.exc cimport PyErr_Clear
from cpython.bytes cimport PyBytes_FromStringAndSize

# Import draken and pyarrow for creating vectors
try:
    import opteryx.draken as draken
    import pyarrow as pa
    DRAKEN_AVAILABLE = True
except ImportError:
    DRAKEN_AVAILABLE = False

# Internal fast array parser (no runtime deps). Parses a JSON array encoded
# in UTF-8 bytes into Python lists. Objects found inside arrays are returned
# as raw bytes; strings are unescaped to Python str; numbers become int/float;
# null -> None, true/false -> bool.


# (removed unused fast whitespace skip function)


def _parse_array_from_bytes(bytes b):
    cdef Py_ssize_t i = 0
    cdef Py_ssize_t n = len(b)

    def parse_value():
        nonlocal i
        # skip whitespace
        while i < n and b[i] in (32,9,10,13):
            i += 1
        if i >= n:
            raise ValueError('unexpected end')
        c = b[i]
        # string
        if c == 34:  # '"'
            i += 1
            chars = []
            while i < n:
                ch = b[i]
                if ch == 34:
                    i += 1
                    return ''.join(chars)
                if ch == 92:  # backslash
                    i += 1
                    if i >= n:
                        raise ValueError('unterminated escape')
                    esc = b[i]
                    i += 1
                    if esc == 34: chars.append('"')
                    elif esc == 92: chars.append('\\')
                    elif esc == 47: chars.append('/')
                    elif esc == 98: chars.append('\b')
                    elif esc == 102: chars.append('\f')
                    elif esc == 110: chars.append('\n')
                    elif esc == 114: chars.append('\r')
                    elif esc == 116: chars.append('\t')
                    elif esc == 117:
                        # unicode escape \uXXXX
                        if i + 4 <= n:
                            hex_s = b[i:i+4].decode('ascii')
                            try:
                                cp = int(hex_s, 16)
                                chars.append(chr(cp))
                            except Exception:
                                chars.append('\\u' + hex_s)
                            i += 4
                        else:
                            raise ValueError('invalid unicode escape')
                    else:
                        # unknown escape, keep char
                        chars.append(chr(esc))
                else:
                    # append raw utf-8 byte; accumulate then decode at end
                    # to support multi-byte UTF-8 sequences, collect bytes
                    start = i
                    # collect consecutive non-escape non-quote bytes
                    while i < n and b[i] != 34 and b[i] != 92:
                        i += 1
                    # decode slice
                    chars.append(b[start:i].decode('utf-8'))
            raise ValueError('unterminated string')

        # null
        if c == 110 and i + 4 <= n and b[i:i+4] == b'null':
            i += 4
            return None

        # true/false
        if c == 116 and i + 4 <= n and b[i:i+4] == b'true':
            i += 4
            return True
        if c == 102 and i + 5 <= n and b[i:i+5] == b'false':
            i += 5
            return False

        # number
        if c == 45 or (48 <= c <= 57):
            start = i
            if c == 45:
                i += 1
            while i < n and 48 <= b[i] <= 57:
                i += 1
            if i < n and b[i] == 46:
                i += 1
                while i < n and 48 <= b[i] <= 57:
                    i += 1
            if i < n and (b[i] == 101 or b[i] == 69):
                i += 1
                if i < n and (b[i] == 43 or b[i] == 45):
                    i += 1
                while i < n and 48 <= b[i] <= 57:
                    i += 1
                return float(b[start:i].decode('ascii'))
            s = b[start:i].decode('ascii')
            if '.' in s or 'e' in s or 'E' in s:
                return float(s)
            else:
                try:
                    return int(s)
                except Exception:
                    return float(s)

        # array
        if c == 91:  # '['
            # parse nested array
            i += 1
            res = []
            # skip whitespace
            while i < n and b[i] in (32,9,10,13): i += 1
            if i < n and b[i] == 93:
                i += 1
                return res
            while True:
                val = parse_value()
                res.append(val)
                while i < n and b[i] in (32,9,10,13): i += 1
                if i >= n:
                    raise ValueError('unterminated array')
                if b[i] == 44:
                    i += 1
                    continue
                elif b[i] == 93:
                    i += 1
                    break
                else:
                    raise ValueError('invalid array separator')
            return res

        # object: return raw bytes slice for object
        if c == 123:  # '{'
            start = i
            depth = 0
            while i < n:
                ch = b[i]
                if ch == 34:
                    # skip string
                    i += 1
                    while i < n:
                        if b[i] == 92:
                            i += 2
                        elif b[i] == 34:
                            i += 1
                            break
                        else:
                            i += 1
                    continue
                if ch == 123:
                    depth += 1
                elif ch == 125:
                    depth -= 1
                    if depth == 0:
                        i += 1
                        return b[start:i]
                i += 1
            raise ValueError('unterminated object')

        raise ValueError('unexpected token at %d' % i)

    # top-level: expect '['
    while i < n and b[i] in (32,9,10,13): i += 1
    if i >= n or b[i] != 91:
        raise ValueError('not an array')
    return parse_value()

cdef extern from "decode.hpp":
    cdef enum JsonType:
        pass
    cdef cppclass ColumnSchema:
        string name
        JsonType type
        bint nullable
        JsonType element_type
    cdef cppclass JsonlColumn:
        vector[int64_t] int_values
        vector[double] double_values
        vector[string] string_values
        vector[uint8_t] boolean_values
        vector[uint8_t] null_mask
        string type
        bint success
    cdef cppclass JsonlTable:
        vector[JsonlColumn] columns
        vector[string] column_names
        size_t num_rows
        bint success
    vector[ColumnSchema] GetJsonlSchema(const uint8_t* data, size_t size, size_t sample_size) except +
    JsonlTable ReadJsonl(const uint8_t* data, size_t size, const vector[string]& column_names) except +
    JsonlTable ReadJsonl(const uint8_t* data, size_t size) except +
    PyObject* ParseJsonSliceToPyObject(const uint8_t* data, size_t len, bint parse_objects)

def get_jsonl_schema(data, sample_size=25):
    """
    Infer the schema of a JSONL dataset from a sample of the data.

    Parameters
    ----------
    data : bytes or object supporting the buffer protocol
        The JSONL data to analyze.
    sample_size : int, optional
        The number of rows to sample for schema inference (default: 25).

    Returns
    -------
    list of dict
        A list of dictionaries, each describing a column with keys:
        - 'name': str, the column name
        - 'type': str, the inferred type ('null', 'boolean', 'int64', 'double', 'bytes')
        - 'nullable': bool, whether the column can contain null values
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef bint have_view = False
    if isinstance(data, bytes):
        data_bytes = <bytes>data
        data_ptr = <const uint8_t*>(<char*>data_bytes)
        data_size = len(data_bytes)
    else:
        if PyObject_GetBuffer(data, &view, PyBUF_CONTIG_RO) == -1:
            raise TypeError("object does not support contiguous buffer interface")
        have_view = True
        data_ptr = <const uint8_t*>view.buf
        data_size = <size_t>view.len
    cdef vector[ColumnSchema] schema = GetJsonlSchema(data_ptr, data_size, sample_size)
    if have_view:
        PyBuffer_Release(&view)
    result = []
    cdef size_t i
    cdef int type_val
    for i in range(schema.size()):
        col = schema[i]
        type_val = <int>col.type
        type_str = "string"
        # JsonType enum: Null=0, Boolean=1, Integer=2, Double=3, String=4, Array=5, Object=6
        if type_val == 0:
            type_str = "null"
        elif type_val == 1:
            type_str = "boolean"
        elif type_val == 2:
            type_str = "int64"
        elif type_val == 3:
            type_str = "double"
        elif type_val == 4:
            type_str = "string"
        elif type_val == 5:
            # array: include element type if available
            elem_val = <int>col.element_type
            if elem_val == 2:
                type_str = "array<int64>"
            elif elem_val == 3:
                type_str = "array<double>"
            elif elem_val == 4:
                type_str = "array<bytes>"
            else:
                type_str = "array"
        elif type_val == 6:
            type_str = "object"
        result.append({
            'name': col.name.decode('utf-8'),
            'type': type_str,
            'nullable': col.nullable
        })
    return result


def read_jsonl(data, columns=None, parse_arrays=True, parse_objects=True):
    """
    Reads a JSONL (JSON Lines) dataset and returns its contents in a columnar format.

    Parameters
    ----------
    data : bytes or object supporting buffer protocol
        The JSONL data to read. Can be a bytes object or any object supporting the buffer protocol.
    columns : list of str, optional
        List of column names to read. If None, all columns are read.

    Returns
    -------
    dict
        A dictionary with the following keys:
            - 'success': bool, True if reading was successful.
            - 'column_names': list of str, names of the columns.
            - 'num_rows': int, number of rows in the dataset.
            - 'columns': list, each element is a list of values for a column (with None for nulls), or None if the column failed to read.
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef bint have_view = False
    if isinstance(data, bytes):
        data_bytes = <bytes>data
        data_ptr = <const uint8_t*>(<char*>data_bytes)
        data_size = len(data_bytes)
    else:
        if PyObject_GetBuffer(data, &view, PyBUF_CONTIG_RO) == -1:
            raise TypeError("object does not support contiguous buffer interface")
        have_view = True
        data_ptr = <const uint8_t*>view.buf
        data_size = <size_t>view.len
    cdef vector[string] column_names_vec
    cdef JsonlTable table
    if columns is None:
        table = ReadJsonl(data_ptr, data_size)
    else:
        for col_name in columns:
            column_names_vec.push_back(col_name.encode('utf-8'))
        table = ReadJsonl(data_ptr, data_size, column_names_vec)
    if have_view:
        PyBuffer_Release(&view)
    if not table.success:
        return {
            'success': False,
            'column_names': [],
            'num_rows': 0,
            'columns': []
        }
    py_column_names = []
    cdef size_t i
    for i in range(table.column_names.size()):
        py_column_names.append(table.column_names[i].decode('utf-8'))
    py_columns = []
    # Python-level temporary for simdjson results
    o = None
    cdef PyObject* o_ptr
    cdef JsonlColumn* col
    for i in range(table.columns.size()):
        col = &table.columns[i]
        if not col.success:
            py_columns.append(None)
            continue
        col_type = col.type.decode('utf-8')
        if col_type == 'int64':
            py_list = []
            for j in range(col.int_values.size()):
                if col.null_mask[j]:
                    py_list.append(None)
                else:
                    py_list.append(col.int_values[j])
            py_columns.append(py_list)
        elif col_type == 'double':
            py_list = []
            for j in range(col.double_values.size()):
                if col.null_mask[j]:
                    py_list.append(None)
                else:
                    py_list.append(col.double_values[j])
            py_columns.append(py_list)
        elif col_type == 'string' or col_type == 'bytes':
            # Bytes columns: return as bytes (binary data), do NOT parse as JSON
            # The schema has already determined this is a bytes/string column, not array/object
            py_list = []
            for j in range(col.string_values.size()):
                if col.null_mask[j]:
                    py_list.append(None)
                    continue

                raw = col.string_values[j]
                if raw.size() == 0:
                    py_list.append(b'')
                    continue

                # Always return as bytes, never parse as JSON
                py_obj = <object>PyBytes_FromStringAndSize(<const char*>raw.data(), <Py_ssize_t>raw.size())
                if py_obj is not None:
                    py_list.append(py_obj)
                else:
                    py_list.append(b'')
            py_columns.append(py_list)
        elif col_type.startswith('array'):
            # Array columns: may be annotated as array<elemtype>
            # Determine element type if provided (e.g. array<bytes>)
            elem_type = None
            if col_type.startswith('array<') and col_type.endswith('>'):
                elem_type = col_type[6:-1]

            def _convert_strings_to_bytes_inplace(obj):
                # Recursively convert str elements in lists to bytes when elem_type == 'bytes'
                # obj is a Python object returned from the parser
                if isinstance(obj, list):
                    for idx in range(len(obj)):
                        v = obj[idx]
                        if isinstance(v, str):
                            obj[idx] = v.encode('utf-8')
                        elif isinstance(v, list):
                            _convert_strings_to_bytes_inplace(v)
                        # leave dicts/bytes as-is

            py_list = []
            for j in range(col.string_values.size()):
                if col.null_mask[j]:
                    py_list.append(None)
                    continue

                raw = col.string_values[j]
                if raw.size() == 0:
                    py_list.append([])
                    continue

                if parse_arrays:
                    # Parse the JSON array into Python list
                    o_ptr = ParseJsonSliceToPyObject(<const uint8_t*>raw.data(), raw.size(), parse_objects)
                    if o_ptr != NULL:
                        o = <object>o_ptr
                        # If element type is bytes, or unspecified but the parsed
                        # array contains string elements (likely binary-as-JSON
                        # strings), convert those strings to bytes.
                        if isinstance(o, list):
                            if elem_type == 'bytes':
                                _convert_strings_to_bytes_inplace(o)
                            elif elem_type is None:
                                # Heuristic: if at least one leaf element is str,
                                # convert all string leaves to bytes
                                def _has_string_leaf(x):
                                    if isinstance(x, list):
                                        for y in x:
                                            if _has_string_leaf(y):
                                                return True
                                        return False
                                    return isinstance(x, str)
                                if _has_string_leaf(o):
                                    _convert_strings_to_bytes_inplace(o)
                        py_list.append(o)
                    else:
                        if PyErr_Occurred():
                            PyErr_Clear()
                        try:
                            parsed = _parse_array_from_bytes(raw)
                            if isinstance(parsed, list):
                                if elem_type == 'bytes':
                                    _convert_strings_to_bytes_inplace(parsed)
                                elif elem_type is None:
                                    def _has_string_leaf(x):
                                        if isinstance(x, list):
                                            for y in x:
                                                if _has_string_leaf(y):
                                                    return True
                                            return False
                                        return isinstance(x, str)
                                    if _has_string_leaf(parsed):
                                        _convert_strings_to_bytes_inplace(parsed)
                            py_list.append(parsed)
                        except Exception:
                            # Fallback to raw string
                            py_list.append(raw.decode('utf-8'))
                else:
                    # Return as string without parsing
                    py_list.append(raw.decode('utf-8'))
            py_columns.append(py_list)
        elif col_type == 'object':
            # Object columns: return as JSONB (bytes), may contain objects, arrays, or mixed
            # Check each value and handle appropriately
            py_list = []
            for j in range(col.string_values.size()):
                if col.null_mask[j]:
                    py_list.append(None)
                    continue

                raw = col.string_values[j]
                if raw.size() == 0:
                    py_list.append(b'{}')
                    continue

                # Check what type of JSON value this is
                first = raw[0]
                
                if first == 91:  # ord('[')
                    # This is an array in a mixed column
                    if parse_arrays:
                        # Parse as array into Python list
                        o_ptr = ParseJsonSliceToPyObject(<const uint8_t*>raw.data(), raw.size(), parse_objects)
                        if o_ptr != NULL:
                            o = <object>o_ptr
                            py_list.append(o)
                        else:
                            if PyErr_Occurred():
                                PyErr_Clear()
                            try:
                                parsed = _parse_array_from_bytes(raw)
                                py_list.append(parsed)
                            except Exception:
                                py_list.append(raw.decode('utf-8'))
                    else:
                        # Arrays not requested, return as string
                        py_list.append(raw.decode('utf-8'))
                        
                elif first == 123:  # ord('{')
                    # This is an object - always return as JSONB (bytes)
                    py_obj = <object>PyBytes_FromStringAndSize(<const char*>raw.data(), <Py_ssize_t>raw.size())
                    if py_obj is not None:
                        py_list.append(py_obj)
                    else:
                        py_list.append(b'{}')
                else:
                    # Unexpected - fallback to bytes
                    py_obj = <object>PyBytes_FromStringAndSize(<const char*>raw.data(), <Py_ssize_t>raw.size())
                    if py_obj is not None:
                        py_list.append(py_obj)
                    else:
                        py_list.append(b'')
            py_columns.append(py_list)
        elif col_type == 'boolean':
            py_list = []
            for j in range(col.boolean_values.size()):
                if col.null_mask[j]:
                    py_list.append(None)
                else:
                    py_list.append(bool(col.boolean_values[j]))
            py_columns.append(py_list)
        else:
            py_columns.append(None)
    
    # Convert Python lists to draken vectors
    if DRAKEN_AVAILABLE:
        draken_columns = []
        for i in range(len(py_columns)):
            if py_columns[i] is None:
                draken_columns.append(None)
                continue
            
            # Get the column type to determine appropriate Arrow type
            col_type = table.columns[i].type.decode('utf-8')
            
            # Convert Python list to PyArrow array, then to draken vector
            try:
                if col_type == 'int64':
                    arrow_array = pa.array(py_columns[i], type=pa.int64())
                elif col_type == 'double':
                    arrow_array = pa.array(py_columns[i], type=pa.float64())
                elif col_type == 'boolean':
                    arrow_array = pa.array(py_columns[i], type=pa.bool_())
                elif col_type == 'string' or col_type == 'bytes':
                    # String/bytes columns are stored as binary in draken.
                    # This preserves the original UTF-8 bytes without decoding,
                    # since draken's StringVector expects binary data.
                    arrow_array = pa.array(py_columns[i], type=pa.binary())
                elif col_type == 'object':
                    # Object columns are stored as binary (JSONB)
                    arrow_array = pa.array(py_columns[i], type=pa.binary())
                elif col_type.startswith('array'):
                    # Array columns can be converted to draken ArrayVector if typed
                    try:
                        if col_type == 'array<int64>':
                            arrow_array = pa.array(py_columns[i], type=pa.list_(pa.int64()))
                        elif col_type == 'array<double>':
                            arrow_array = pa.array(py_columns[i], type=pa.list_(pa.float64()))
                        elif col_type == 'array<bytes>':
                            arrow_array = pa.array(py_columns[i], type=pa.list_(pa.binary()))
                        else:
                            # Generic array type - keep as Python list
                            # (contains mixed types or nested structures)
                            draken_columns.append(py_columns[i])
                            continue
                    except Exception:
                        # If conversion fails, keep as Python list
                        draken_columns.append(py_columns[i])
                        continue
                else:
                    # Unknown type, keep as Python list
                    draken_columns.append(py_columns[i])
                    continue
                
                # Convert Arrow array to draken vector
                draken_vec = draken.Vector.from_arrow(arrow_array)
                draken_columns.append(draken_vec)
            except Exception:
                # If conversion fails, keep as Python list
                draken_columns.append(py_columns[i])
        
        return {
            'success': True,
            'column_names': py_column_names,
            'num_rows': table.num_rows,
            'columns': draken_columns
        }
    else:
        # Draken not available, return Python lists as before
        return {
            'success': True,
            'column_names': py_column_names,
            'num_rows': table.num_rows,
            'columns': py_columns
        }
