// CSV/TSV Reader Header
#pragma once
#include <Python.h>
#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include <string_view>

// Column value types that we support for CSV
enum class CsvType {
    Null,
    Boolean,
    Integer,
    Double,
    String
};

// Schema information for a column
struct CsvColumnSchema {
    std::string name;
    CsvType type;
    bool nullable = true;
};

// Structure to hold decoded column data
struct CsvColumn {
    std::vector<int64_t> int_values;
    std::vector<double> double_values;
    std::vector<std::string> string_values;
    std::vector<uint8_t> boolean_values;
    std::vector<uint8_t> null_mask;  // 1 = null, 0 = not null
    std::string type;  // "int64", "double", "string", "boolean"
    bool success = false;
};

// Structure to hold a decoded CSV table
struct CsvTable {
    std::vector<CsvColumn> columns;
    std::vector<std::string> column_names;
    size_t num_rows = 0;
    bool success = false;
};

// Dialect options for CSV parsing
struct CsvDialect {
    char delimiter = ',';     // Field delimiter (comma for CSV, tab for TSV)
    char quote_char = '"';    // Quote character
    char escape_char = '\\';  // Escape character (often same as quote_char)
    bool double_quote = true; // Use "" to escape quotes instead of \"
    bool has_header = true;   // First line contains column names
};

// Get schema from CSV data with type inference
std::vector<CsvColumnSchema> GetCsvSchema(
    const uint8_t* data, 
    size_t size, 
    const CsvDialect& dialect,
    size_t sample_size = 100
);

// Get schema using already-parsed header names. This avoids reparsing the header
// when the caller has already consumed it. 'data' should point to the start of
// the rows (i.e. immediately after the header) and 'size' should be the remaining
// size.
std::vector<CsvColumnSchema> GetCsvSchema(
    const uint8_t* data,
    size_t size,
    const CsvDialect& dialect,
    const std::vector<std::string>& column_names,
    size_t sample_size = 100
);

// Read CSV data with optional column projection
CsvTable ReadCsv(
    const uint8_t* data, 
    size_t size,
    const CsvDialect& dialect,
    const std::vector<std::string>& column_names
);

// Overload that reads all columns
CsvTable ReadCsv(
    const uint8_t* data, 
    size_t size,
    const CsvDialect& dialect
);

// Auto-detect CSV dialect (delimiter, quote char)
CsvDialect DetectCsvDialect(const uint8_t* data, size_t size, size_t sample_size = 100);
