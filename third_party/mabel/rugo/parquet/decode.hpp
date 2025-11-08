#pragma once
#include "metadata.hpp"
#include <cstdint>
#include <string>
#include <vector>

// Structure to hold decoded column data
struct DecodedColumn {
  std::vector<int32_t> int32_values;
  std::vector<int64_t> int64_values;
  std::vector<std::string> string_values;
  std::vector<uint8_t> boolean_values;   // for boolean (using uint8_t instead of bool)
  std::vector<float> float32_values;     // for float32
  std::vector<double> float64_values;    // for float64
  std::string type; // "int32", "int64", "string", "boolean", "float32", "float64"
  bool success = false;
};

// Structure to hold a decoded table
struct DecodedTable {
  std::vector<std::vector<DecodedColumn>> row_groups; // [row_group][column]
  std::vector<std::string> column_names;
  bool success = false;
};

// Check if a parquet file can be decoded with our limited decoder
// Returns true only if:
// - All columns are uncompressed
// - All columns use PLAIN encoding
// - All columns are int32, int64, or string types
bool CanDecode(const std::string &path);

// Check if parquet data in memory can be decoded
bool CanDecode(const uint8_t* data, size_t size);

// NEW PRIMARY API: Read parquet data from memory view with column selection
// Returns a table structure with decoded data organized by row groups and columns
DecodedTable ReadParquet(const uint8_t* data, size_t size, const std::vector<std::string>& column_names);

// Overload that decodes all columns when none are specified
DecodedTable ReadParquet(const uint8_t* data, size_t size);

// Decode a specific column from memory buffer for a specific row group
DecodedColumn DecodeColumnFromMemory(const uint8_t* data, size_t size, 
                                   const std::string &column_name,
                                   const RowGroupStats &row_group, 
                                   int row_group_index);

// Legacy file-based functions (kept for backward compatibility)
DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name, 
                           const RowGroupStats &row_group, int row_group_index);

DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name);
