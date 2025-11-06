"""Cython wrapper for reading CSV/TSV files."""

# distutils: language = c++
# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: boundscheck=False
# cython: wraparound=False
# cython: infer_types=True

from libc.stdint cimport uint8_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp cimport bool as cbool
from cpython.buffer cimport PyBUF_CONTIG_RO, PyObject_GetBuffer, PyBuffer_Release, Py_buffer


# C++ declarations from csv_parser.hpp
cdef extern from "csv_parser.hpp":
    cdef enum CsvType:
        Null "CsvType::Null"
        Boolean "CsvType::Boolean"
        Integer "CsvType::Integer"
        Double "CsvType::Double"
        String "CsvType::String"
    
    cdef cppclass CsvColumnSchema:
        string name
        CsvType type
        cbool nullable
    
    cdef cppclass CsvColumn:
        vector[long long] int_values
        vector[double] double_values
        vector[string] string_values
        vector[uint8_t] boolean_values
        vector[uint8_t] null_mask
        string type
        cbool success
    
    cdef cppclass CsvTable:
        vector[CsvColumn] columns
        vector[string] column_names
        size_t num_rows
        cbool success
    
    cdef cppclass CsvDialect:
        char delimiter
        char quote_char
        char escape_char
        cbool double_quote
        cbool has_header
    
    vector[CsvColumnSchema] GetCsvSchema(const uint8_t* data, size_t size, 
                                         const CsvDialect& dialect, size_t sample_size)
    
    CsvTable ReadCsv(const uint8_t* data, size_t size, const CsvDialect& dialect)
    
    CsvTable ReadCsv(const uint8_t* data, size_t size, const CsvDialect& dialect,
                     const vector[string]& column_names)
    
    CsvDialect DetectCsvDialect(const uint8_t* data, size_t size, size_t sample_size)


def get_csv_schema(data, delimiter=',', quote_char='"', sample_size=100, has_header=True):
    """
    Extract schema information from CSV data.
    
    Parameters
    ----------
    data : bytes or memoryview
        The CSV data to analyze
    delimiter : str, default ','
        Field delimiter character (use '\\t' for TSV)
    quote_char : str, default '"'
        Quote character for fields
    sample_size : int, default 100
        Number of rows to sample for type inference
    has_header : bool, default True
        Whether the first line contains column names
    
    Returns
    -------
    list of dict
        List of column schemas with keys: name, type, nullable
        
    Examples
    --------
    >>> data = b'name,age,salary\\nAlice,30,50000\\nBob,25,45000'
    >>> schema = get_csv_schema(data)
    >>> for col in schema:
    ...     print(f"{col['name']}: {col['type']}")
    name: string
    age: int64
    salary: int64
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef cbool have_view = False
    
    # Handle different input types
    if isinstance(data, memoryview):
        PyObject_GetBuffer(data.obj, &view, PyBUF_CONTIG_RO)
        data_ptr = <const uint8_t*>view.buf
        data_size = view.len
        have_view = True
    elif isinstance(data, bytes):
        data_bytes = data
        data_ptr = <const uint8_t*><char*>data_bytes
        data_size = len(data_bytes)
    else:
        raise TypeError(f"Expected bytes or memoryview, got {type(data)}")
    
    # Setup dialect
    cdef CsvDialect dialect
    dialect.delimiter = ord(delimiter[0]) if delimiter else ord(',')
    dialect.quote_char = ord(quote_char[0]) if quote_char else ord('"')
    dialect.escape_char = ord('\\')
    dialect.double_quote = True
    dialect.has_header = has_header
    
    # Get schema
    cdef vector[CsvColumnSchema] schema = GetCsvSchema(data_ptr, data_size, dialect, sample_size)
    
    if have_view:
        PyBuffer_Release(&view)
    
    # Convert to Python
    result = []
    cdef size_t i
    cdef int type_val
    
    for i in range(schema.size()):
        type_val = <int>schema[i].type
        
        if type_val == <int>CsvType.Integer:
            type_str = "int64"
        elif type_val == <int>CsvType.Double:
            type_str = "double"
        elif type_val == <int>CsvType.Boolean:
            type_str = "boolean"
        elif type_val == <int>CsvType.String:
            type_str = "string"
        else:
            type_str = "string"
        
        result.append({
            'name': schema[i].name.decode('utf-8'),
            'type': type_str,
            'nullable': schema[i].nullable
        })
    
    return result


def read_csv(data, columns=None, delimiter=',', quote_char='"', has_header=True):
    """
    Read CSV data into columnar format.
    
    Parameters
    ----------
    data : bytes or memoryview
        The CSV data to read
    columns : list of str, optional
        List of column names to read. If None, reads all columns.
    delimiter : str, default ','
        Field delimiter character (use '\\t' for TSV)
    quote_char : str, default '"'
        Quote character for fields
    has_header : bool, default True
        Whether the first line contains column names
    
    Returns
    -------
    dict
        Dictionary with keys:
        - success: bool - Whether reading succeeded
        - num_rows: int - Number of rows read
        - column_names: list of str - Column names
        - columns: list of list - Column data (one list per column)
        
    Examples
    --------
    >>> data = b'name,age,salary\\nAlice,30,50000\\nBob,25,45000'
    >>> result = read_csv(data)
    >>> print(result['column_names'])
    ['name', 'age', 'salary']
    >>> print(result['columns'][0])  # name column
    ['Alice', 'Bob']
    
    >>> # Read with projection
    >>> result = read_csv(data, columns=['name', 'salary'])
    >>> print(result['column_names'])
    ['name', 'salary']
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef cbool have_view = False
    
    # Handle different input types
    if isinstance(data, memoryview):
        PyObject_GetBuffer(data.obj, &view, PyBUF_CONTIG_RO)
        data_ptr = <const uint8_t*>view.buf
        data_size = view.len
        have_view = True
    elif isinstance(data, bytes):
        data_bytes = data
        data_ptr = <const uint8_t*><char*>data_bytes
        data_size = len(data_bytes)
    else:
        raise TypeError(f"Expected bytes or memoryview, got {type(data)}")
    
    # Setup dialect
    cdef CsvDialect dialect
    dialect.delimiter = ord(delimiter[0]) if delimiter else ord(',')
    dialect.quote_char = ord(quote_char[0]) if quote_char else ord('"')
    dialect.escape_char = ord('\\')
    dialect.double_quote = True
    dialect.has_header = has_header
    
    # Convert column names to C++ vector
    cdef vector[string] column_names_cpp
    if columns is not None:
        for col in columns:
            column_names_cpp.push_back(col.encode('utf-8'))
    
    # Read CSV
    cdef CsvTable table
    if columns is None:
        table = ReadCsv(data_ptr, data_size, dialect)
    else:
        table = ReadCsv(data_ptr, data_size, dialect, column_names_cpp)
    
    if have_view:
        PyBuffer_Release(&view)
    
    # Convert to Python
    result = {
        'success': table.success,
        'num_rows': table.num_rows,
        'column_names': [],
        'columns': []
    }
    
    cdef size_t i, j
    
    # Column names
    for i in range(table.column_names.size()):
        result['column_names'].append(table.column_names[i].decode('utf-8'))
    
    # Column data
    for i in range(table.columns.size()):
        col_type = table.columns[i].type.decode('utf-8')
        # Pre-allocate list for faster assignment and fewer resizes
        if col_type == "int64":
            n = table.columns[i].int_values.size()
            nm = table.columns[i].null_mask.size()
            col_data = [None] * n
            for j in range(n):
                if nm == n:
                    if table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = table.columns[i].int_values[j]
                else:
                    if j < nm and table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = table.columns[i].int_values[j]
        elif col_type == "double":
            n = table.columns[i].double_values.size()
            nm = table.columns[i].null_mask.size()
            col_data = [None] * n
            for j in range(n):
                if nm == n:
                    if table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = table.columns[i].double_values[j]
                else:
                    if j < nm and table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = table.columns[i].double_values[j]
        elif col_type == "boolean":
            n = table.columns[i].boolean_values.size()
            nm = table.columns[i].null_mask.size()
            col_data = [None] * n
            for j in range(n):
                if nm == n:
                    if table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = bool(table.columns[i].boolean_values[j])
                else:
                    if j < nm and table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = bool(table.columns[i].boolean_values[j])
        else:  # string
            n = table.columns[i].string_values.size()
            nm = table.columns[i].null_mask.size()
            col_data = [None] * n
            for j in range(n):
                if nm == n:
                    if table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = table.columns[i].string_values[j].decode('utf-8')
                else:
                    if j < nm and table.columns[i].null_mask[j]:
                        col_data[j] = None
                    else:
                        col_data[j] = table.columns[i].string_values[j].decode('utf-8')

        result['columns'].append(col_data)
    
    return result


def detect_csv_dialect(data, sample_size=100):
    """
    Auto-detect CSV dialect (delimiter, quote character).
    
    Parameters
    ----------
    data : bytes or memoryview
        The CSV data to analyze
    sample_size : int, default 100
        Number of rows to sample for detection
    
    Returns
    -------
    dict
        Dictionary with detected dialect parameters:
        - delimiter: str - Detected delimiter character
        - quote_char: str - Quote character
        
    Examples
    --------
    >>> data = b'name\\tage\\tsalary\\nAlice\\t30\\t50000'
    >>> dialect = detect_csv_dialect(data)
    >>> print(dialect['delimiter'])
    '\t'
    """
    cdef const uint8_t* data_ptr
    cdef size_t data_size
    cdef bytes data_bytes
    cdef Py_buffer view
    cdef cbool have_view = False
    
    # Handle different input types
    if isinstance(data, memoryview):
        PyObject_GetBuffer(data.obj, &view, PyBUF_CONTIG_RO)
        data_ptr = <const uint8_t*>view.buf
        data_size = view.len
        have_view = True
    elif isinstance(data, bytes):
        data_bytes = data
        data_ptr = <const uint8_t*><char*>data_bytes
        data_size = len(data_bytes)
    else:
        raise TypeError(f"Expected bytes or memoryview, got {type(data)}")
    
    # Detect dialect
    cdef CsvDialect dialect = DetectCsvDialect(data_ptr, data_size, sample_size)
    
    if have_view:
        PyBuffer_Release(&view)
    
    return {
        'delimiter': chr(dialect.delimiter),
        'quote_char': chr(dialect.quote_char)
    }


# Convenience function for TSV
def read_tsv(data, columns=None):
    """
    Read TSV (tab-separated values) data.
    
    This is a convenience wrapper around read_csv with delimiter='\\t'.
    
    Parameters
    ----------
    data : bytes or memoryview
        The TSV data to read
    columns : list of str, optional
        List of column names to read. If None, reads all columns.
    
    Returns
    -------
    dict
        Same as read_csv()
        
    Examples
    --------
    >>> data = b'name\\tage\\tsalary\\nAlice\\t30\\t50000'
    >>> result = read_tsv(data)
    >>> print(result['column_names'])
    ['name', 'age', 'salary']
    """
    return read_csv(data, columns=columns, delimiter='\t')


def get_tsv_schema(data, sample_size=100):
    """
    Extract schema from TSV data.
    
    This is a convenience wrapper around get_csv_schema with delimiter='\\t'.
    
    Parameters
    ----------
    data : bytes or memoryview
        The TSV data to analyze
    sample_size : int, default 100
        Number of rows to sample for type inference
    
    Returns
    -------
    list of dict
        List of column schemas
    """
    return get_csv_schema(data, delimiter='\t', sample_size=sample_size)
