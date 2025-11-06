# parquet_reader.pxd
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map


cdef extern from "metadata.hpp":
    cdef cppclass MetadataParseOptions:
        bint schema_only
        bint include_statistics
        long long max_row_groups
        MetadataParseOptions() except +

    cdef cppclass ColumnStats:
        string name
        string physical_type
        string logical_type
        
        # Sizes & counts
        int64_t num_values
        int64_t total_uncompressed_size
        int64_t total_compressed_size
        
        # Offsets
        int64_t data_page_offset
        int64_t index_page_offset
        int64_t dictionary_page_offset
        
        # Statistics
        bint has_min
        bint has_max
        string min
        string max
        int64_t null_count
        int64_t distinct_count
        
        # Bloom filter
        int64_t bloom_offset
        int64_t bloom_length
        
        # Encodings & codec
        vector[int32_t] encodings
        int32_t codec
        
        # Key/value metadata
        unordered_map[string, string] key_value_metadata

    cdef cppclass RowGroupStats:
        long long num_rows
        long long total_byte_size
        vector[ColumnStats] columns

    cdef cppclass SchemaElement:
        string name
        string full_name
        string physical_type
        string logical_type
        int num_children
        int type_length
        int scale
        int precision
        int repetition_type
        vector[SchemaElement] children

    cdef cppclass SchemaField:
        string name
        string physical_type
        string logical_type
        bint nullable

    cdef cppclass FileStats:
        long long num_rows
        vector[RowGroupStats] row_groups
        vector[SchemaElement] schema
        vector[SchemaField] schema_columns

    FileStats ReadParquetMetadataC(const char* path)
    FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size)
    FileStats ReadParquetMetadataC(const char* path, const MetadataParseOptions& options)
    FileStats ReadParquetMetadata(const string& path, const MetadataParseOptions& options)
    FileStats ReadParquetMetadata(const string& path)
    FileStats ReadParquetMetadataFromBuffer(const uint8_t* buf, size_t size, const MetadataParseOptions& options)
    bint TestBloomFilter(const string& file_path, long long bloom_offset, long long bloom_length, const string& value)
    
    # Helper functions
    const char* EncodingToString(int32_t enc)
    const char* CompressionCodecToString(int32_t codec)

cdef extern from "decode.hpp":
    cdef cppclass DecodedColumn:
        vector[int32_t] int32_values
        vector[int64_t] int64_values
        vector[string] string_values
        vector[uint8_t] boolean_values
        vector[float] float32_values
        vector[double] float64_values
        string type
        bint success
    
    cdef cppclass DecodedTable:
        vector[vector[DecodedColumn]] row_groups  # [row_group][column]
        vector[string] column_names
        bint success
    
    bint CanDecode(const string& path)
    bint CanDecode(const uint8_t* data, size_t size)
    
    # New memory-based functions
    DecodedColumn DecodeColumnFromMemory(const uint8_t* data, size_t size, const string& column_name, const RowGroupStats& row_group, int row_group_index)
    DecodedTable ReadParquet(const uint8_t* data, size_t size, const vector[string]& column_names)
    DecodedTable ReadParquet(const uint8_t* data, size_t size)
