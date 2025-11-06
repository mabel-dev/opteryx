// CSV/TSV Reader Implementation with SIMD optimizations
#include "csv_parser.hpp"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

// Reuse SIMD text search helpers from jsonl module
#include "../jsonl/text_search.hpp"

// SIMD includes for x86-64
#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define HAS_SIMD 1
#endif

namespace {

// We reuse simd::FindChar/FindQuote/FindNewline from `rugo/jsonl/text_search.hpp`
// No local FindDelimiterOrNewline implementation.

// Parse a CSV field, handling quotes and escapes
bool ParseField(const char*& pos, const char* end, const CsvDialect& dialect,
                std::string& out_value, bool& is_quoted) {
    out_value.clear();
    is_quoted = false;
    
    if (pos >= end) return false;
    
    // Skip leading whitespace (optional - RFC 4180 doesn't require this)
    while (pos < end && (*pos == ' ' || *pos == '\t')) {
        pos++;
    }
    
    if (pos >= end) return false;
    
    // Check if field is quoted
    if (*pos == dialect.quote_char) {
        is_quoted = true;
        pos++; // Skip opening quote
        
        // Parse quoted field using SIMD helpers where available.
        while (pos < end) {
            // Try to find the next quote or backslash quickly
            const char* next = simd::FindQuote(pos, end - pos);
            if (!next) {
                // No quote found in remaining data: append rest and return
                out_value.append(pos, end - pos);
                pos = end;
                return true;
            }

            // Append chunk before the special char
            if (next > pos) {
                out_value.append(pos, next - pos);
            }

            // Inspect the special character
            char c = *next;
            pos = next + 1; // advance past this char for further processing

            if (c == dialect.quote_char) {
                // If double-quote escaping is enabled and next char is a quote, add one quote
                if (dialect.double_quote && pos < end && *pos == dialect.quote_char) {
                    out_value.push_back(dialect.quote_char);
                    pos++; // consume the escaped quote
                    continue;
                }

                // Check if the quote is followed by delimiter or newline (end of quoted field)
                if (pos >= end) {
                    return true; // end of data after closing quote
                }
                if (*pos == dialect.delimiter || *pos == '\n' || *pos == '\r') {
                    return true; // end of field
                }
                // Otherwise, treat as closing quote; the calling code will handle next char
                return true;
            } else if (c == dialect.escape_char && !dialect.double_quote) {
                // Escape char: append next char literally if present
                if (pos < end) {
                    out_value.push_back(*pos);
                    pos++;
                } else {
                    return false; // incomplete escape
                }
            }
        }
        return true; // reached end of data inside quoted field
    } else {
        // Parse unquoted field
        const char* field_start = pos;
        // Use SIMD helpers to find delimiter or newline faster when available
        const char* next_delim = simd::FindChar(pos, end - pos, dialect.delimiter);
        const char* next_newline = simd::FindNewline(pos, end - pos);
        const char* next_pos = nullptr;
        if (next_delim && next_newline) {
            next_pos = (next_delim < next_newline) ? next_delim : next_newline;
        } else if (next_delim) {
            next_pos = next_delim;
        } else if (next_newline) {
            next_pos = next_newline;
        }

        if (next_pos) {
            out_value = std::string(field_start, next_pos - field_start);
            pos = next_pos;
        } else {
            // No delimiter/newline found: take rest of buffer
            out_value = std::string(field_start, end - field_start);
            pos = end;
        }
        
        // Trim trailing whitespace from unquoted field (optional)
        while (!out_value.empty() && (out_value.back() == ' ' || out_value.back() == '\t')) {
            out_value.pop_back();
        }
        
        return true;
    }
}

// Skip to next line
void SkipToNextLine(const char*& pos, const char* end) {
    while (pos < end && *pos != '\n') {
        pos++;
    }
    if (pos < end && *pos == '\n') {
        pos++;
    }
}

// Infer type from a string value
CsvType InferType(const std::string& value, bool& is_null) {
    is_null = false;
    
    // Check for null/empty
    if (value.empty() || value == "null" || value == "NULL" || value == "None") {
        is_null = true;
        return CsvType::Null;
    }
    
    // Check for boolean
    if (value == "true" || value == "TRUE" || value == "True" || value == "1") {
        return CsvType::Boolean;
    }
    if (value == "false" || value == "FALSE" || value == "False" || value == "0") {
        return CsvType::Boolean;
    }
    
    // Try to parse as integer
    char* end_ptr;
    errno = 0;
    long long int_val = std::strtoll(value.c_str(), &end_ptr, 10);
    if (errno == 0 && end_ptr == value.c_str() + value.size() && *end_ptr == '\0') {
        return CsvType::Integer;
    }
    
    // Try to parse as double
    errno = 0;
    double double_val = std::strtod(value.c_str(), &end_ptr);
    if (errno == 0 && end_ptr == value.c_str() + value.size() && *end_ptr == '\0') {
        return CsvType::Double;
    }
    
    // Default to string
    return CsvType::String;
}

// Parse boolean value
bool ParseBoolean(const std::string& value) {
    return value == "true" || value == "TRUE" || value == "True" || value == "1";
}

} // anonymous namespace

// Detect CSV dialect
CsvDialect DetectCsvDialect(const uint8_t* data, size_t size, size_t sample_size) {
    CsvDialect dialect;
    
    // Sample first N lines to detect delimiter
    const char* pos = reinterpret_cast<const char*>(data);
    const char* end = pos + size;
    
    std::unordered_map<char, int> delimiter_counts;
    std::vector<char> potential_delimiters = {',', '\t', ';', '|', ' '};
    
    size_t lines_sampled = 0;
    while (pos < end && lines_sampled < sample_size) {
        const char* line_start = pos;
        SkipToNextLine(pos, end);
        
        // Count potential delimiters in this line
        for (const char* p = line_start; p < pos; p++) {
            for (char delim : potential_delimiters) {
                if (*p == delim) {
                    delimiter_counts[delim]++;
                }
            }
        }
        lines_sampled++;
    }
    
    // Choose the delimiter that appears most consistently
    char best_delimiter = ',';
    int max_count = 0;
    for (const auto& pair : delimiter_counts) {
        if (pair.second > max_count) {
            max_count = pair.second;
            best_delimiter = pair.first;
        }
    }
    
    dialect.delimiter = best_delimiter;
    return dialect;
}

// Get schema with type inference
std::vector<CsvColumnSchema> GetCsvSchema(
    const uint8_t* data, 
    size_t size, 
    const CsvDialect& dialect,
    size_t sample_size) {
    
    std::vector<CsvColumnSchema> schema;
    
    const char* pos = reinterpret_cast<const char*>(data);
    const char* end = pos + size;
    
    if (pos >= end) return schema;
    
    // Parse header line
    std::vector<std::string> column_names;
    std::string field;
    bool is_quoted;
    
    while (pos < end) {
        if (!ParseField(pos, end, dialect, field, is_quoted)) {
            break;
        }
        column_names.push_back(field);
        
        if (pos < end && *pos == dialect.delimiter) {
            pos++; // Skip delimiter
        } else if (pos < end && (*pos == '\n' || *pos == '\r')) {
            if (*pos == '\r' && pos + 1 < end && pos[1] == '\n') {
                pos += 2; // Skip \r\n
            } else {
                pos++; // Skip \n or \r
            }
            break;
        } else if (pos >= end) {
            break;
        }
    }
    
    // Initialize schema with unknown types
    for (const auto& name : column_names) {
        CsvColumnSchema col_schema;
        col_schema.name = name;
        col_schema.type = CsvType::String; // Default to string
        col_schema.nullable = true;
        schema.push_back(col_schema);
    }
    
    // Sample rows to infer types
    std::vector<std::unordered_set<CsvType>> type_candidates(column_names.size());
    for (size_t i = 0; i < column_names.size(); i++) {
        type_candidates[i].insert(CsvType::Null);
    }
    
    size_t rows_sampled = 0;
    while (pos < end && rows_sampled < sample_size) {
        size_t col_idx = 0;
        
        while (pos < end && col_idx < column_names.size()) {
            if (!ParseField(pos, end, dialect, field, is_quoted)) {
                break;
            }
            
            bool is_null;
            CsvType inferred_type = InferType(field, is_null);
            if (!is_null || field.empty()) {
                type_candidates[col_idx].insert(inferred_type);
            }
            
            col_idx++;
            
            if (pos < end && *pos == dialect.delimiter) {
                pos++; // Skip delimiter
            } else if (pos < end && (*pos == '\n' || *pos == '\r')) {
                if (*pos == '\r' && pos + 1 < end && pos[1] == '\n') {
                    pos += 2; // Skip \r\n
                } else {
                    pos++; // Skip \n or \r
                }
                break;
            } else if (pos >= end) {
                break;
            }
        }
        
        rows_sampled++;
    }
    
    // Determine final types based on candidates
    for (size_t i = 0; i < schema.size(); i++) {
        const auto& candidates = type_candidates[i];
        
        if (candidates.empty() || (candidates.size() == 1 && candidates.count(CsvType::Null))) {
            schema[i].type = CsvType::String;
        } else if (candidates.count(CsvType::String)) {
            schema[i].type = CsvType::String;
        } else if (candidates.count(CsvType::Double)) {
            schema[i].type = CsvType::Double;
        } else if (candidates.count(CsvType::Integer)) {
            schema[i].type = CsvType::Integer;
        } else if (candidates.count(CsvType::Boolean)) {
            schema[i].type = CsvType::Boolean;
        } else {
            schema[i].type = CsvType::String;
        }
    }
    
    return schema;
}

// Overload that uses already-parsed column names (header removed from data)
std::vector<CsvColumnSchema> GetCsvSchema(
    const uint8_t* data,
    size_t size,
    const CsvDialect& dialect,
    const std::vector<std::string>& column_names,
    size_t sample_size) {

    std::vector<CsvColumnSchema> schema;
    if (column_names.empty()) return schema;

    // Initialize schema with unknown types
    for (const auto& name : column_names) {
        CsvColumnSchema col_schema;
        col_schema.name = name;
        col_schema.type = CsvType::String; // Default to string
        col_schema.nullable = true;
        schema.push_back(col_schema);
    }

    const char* pos = reinterpret_cast<const char*>(data);
    const char* end = pos + size;

    std::vector<std::unordered_set<CsvType>> type_candidates(column_names.size());
    for (size_t i = 0; i < column_names.size(); i++) {
        type_candidates[i].insert(CsvType::Null);
    }

    std::string field;
    bool is_quoted;
    size_t rows_sampled = 0;

    while (pos < end && rows_sampled < sample_size) {
        size_t col_idx = 0;
        while (pos < end && col_idx < column_names.size()) {
            if (!ParseField(pos, end, dialect, field, is_quoted)) {
                break;
            }

            bool is_null;
            CsvType inferred_type = InferType(field, is_null);
            if (!is_null || field.empty()) {
                type_candidates[col_idx].insert(inferred_type);
            }

            col_idx++;

            if (pos < end && *pos == dialect.delimiter) {
                pos++; // Skip delimiter
            } else if (pos < end && (*pos == '\n' || *pos == '\r')) {
                if (*pos == '\r' && pos + 1 < end && pos[1] == '\n') {
                    pos += 2; // Skip \r\n
                } else {
                    pos++; // Skip \n or \r
                }
                break;
            } else if (pos >= end) {
                break;
            }
        }
        rows_sampled++;
    }

    // Determine final types
    for (size_t i = 0; i < schema.size(); i++) {
        const auto& candidates = type_candidates[i];
        if (candidates.empty() || (candidates.size() == 1 && candidates.count(CsvType::Null))) {
            schema[i].type = CsvType::String;
        } else if (candidates.count(CsvType::String)) {
            schema[i].type = CsvType::String;
        } else if (candidates.count(CsvType::Double)) {
            schema[i].type = CsvType::Double;
        } else if (candidates.count(CsvType::Integer)) {
            schema[i].type = CsvType::Integer;
        } else if (candidates.count(CsvType::Boolean)) {
            schema[i].type = CsvType::Boolean;
        } else {
            schema[i].type = CsvType::String;
        }
    }

    return schema;
}

// Read CSV with all columns
CsvTable ReadCsv(const uint8_t* data, size_t size, const CsvDialect& dialect) {
    return ReadCsv(data, size, dialect, {});
}

// Read CSV with column projection
CsvTable ReadCsv(
    const uint8_t* data, 
    size_t size,
    const CsvDialect& dialect,
    const std::vector<std::string>& requested_columns) {
    
    CsvTable table;
    
    const char* pos = reinterpret_cast<const char*>(data);
    const char* end = pos + size;
    
    if (pos >= end) {
        table.success = false;
        return table;
    }
    
    // Parse header
    std::vector<std::string> all_column_names;
    std::string field;
    bool is_quoted;
    
    while (pos < end) {
        if (!ParseField(pos, end, dialect, field, is_quoted)) {
            break;
        }
        all_column_names.push_back(field);
        
        if (pos < end && *pos == dialect.delimiter) {
            pos++; // Skip delimiter
        } else if (pos < end && (*pos == '\n' || *pos == '\r')) {
            if (*pos == '\r' && pos + 1 < end && pos[1] == '\n') {
                pos += 2;
            } else {
                pos++;
            }
            break;
        } else if (pos >= end) {
            break;
        }
    }
    
    // Determine which columns to read
    std::vector<size_t> column_indices;
    std::vector<std::string> selected_columns;
    
    if (requested_columns.empty()) {
        // Read all columns
        for (size_t i = 0; i < all_column_names.size(); i++) {
            column_indices.push_back(i);
            selected_columns.push_back(all_column_names[i]);
        }
    } else {
        // Read only requested columns
        for (const auto& req_col : requested_columns) {
            for (size_t i = 0; i < all_column_names.size(); i++) {
                if (all_column_names[i] == req_col) {
                    column_indices.push_back(i);
                    selected_columns.push_back(req_col);
                    break;
                }
            }
        }
    }
    
    // Get schema for selected columns
    // We have already parsed the header into all_column_names; call the
    // overload that avoids reparsing the header. Also, estimate remaining rows
    // by counting newlines and reserve capacity on per-column vectors to reduce
    // reallocations.
    // Find start of data after the header: 'pos' currently points after header
    const char* data_start = pos;
    size_t remaining_size = (data_start < end) ? (end - data_start) : 0;

    // Estimate row count by counting newlines in the remaining data
    size_t estimated_rows = 0;
    for (const char* p = data_start; p < end; ++p) {
        if (*p == '\n') estimated_rows++;
    }

    auto schema = GetCsvSchema((const uint8_t*)data_start, remaining_size, dialect, all_column_names, 100);
    
    // Initialize columns
    table.column_names = selected_columns;
    table.columns.resize(selected_columns.size());
    // Reserve capacity for each column based on estimated rows to avoid frequent reallocations
    for (size_t i = 0; i < table.columns.size(); ++i) {
        table.columns[i].int_values.reserve(estimated_rows);
        table.columns[i].double_values.reserve(estimated_rows);
        table.columns[i].string_values.reserve(estimated_rows);
        table.columns[i].boolean_values.reserve(estimated_rows);
        table.columns[i].null_mask.reserve(estimated_rows);
    }
    for (size_t i = 0; i < selected_columns.size(); i++) {
        size_t col_idx = column_indices[i];
        if (col_idx < schema.size()) {
            switch (schema[col_idx].type) {
                case CsvType::Integer:
                    table.columns[i].type = "int64";
                    break;
                case CsvType::Double:
                    table.columns[i].type = "double";
                    break;
                case CsvType::Boolean:
                    table.columns[i].type = "boolean";
                    break;
                default:
                    table.columns[i].type = "string";
                    break;
            }
        } else {
            table.columns[i].type = "string";
        }
        table.columns[i].success = true;
    }
    
    // Parse data rows
    size_t row_count = 0;
    
    while (pos < end) {
        size_t col_idx = 0;
        
        while (pos < end && col_idx < all_column_names.size()) {
            if (!ParseField(pos, end, dialect, field, is_quoted)) {
                break;
            }
            
            // Check if this column should be read
            auto it = std::find(column_indices.begin(), column_indices.end(), col_idx);
            if (it != column_indices.end()) {
                size_t target_idx = it - column_indices.begin();
                
                // Parse value based on type
                bool is_null;
                CsvType value_type = InferType(field, is_null);
                
                if (is_null || field.empty()) {
                    table.columns[target_idx].null_mask.push_back(1);
                    // Add placeholder values
                    if (table.columns[target_idx].type == "int64") {
                        table.columns[target_idx].int_values.push_back(0);
                    } else if (table.columns[target_idx].type == "double") {
                        table.columns[target_idx].double_values.push_back(0.0);
                    } else if (table.columns[target_idx].type == "boolean") {
                        table.columns[target_idx].boolean_values.push_back(0);
                    } else {
                        table.columns[target_idx].string_values.push_back("");
                    }
                } else {
                    table.columns[target_idx].null_mask.push_back(0);
                    
                    if (table.columns[target_idx].type == "int64") {
                        try {
                            table.columns[target_idx].int_values.push_back(std::stoll(field));
                        } catch (...) {
                            table.columns[target_idx].int_values.push_back(0);
                            table.columns[target_idx].null_mask.back() = 1;
                        }
                    } else if (table.columns[target_idx].type == "double") {
                        try {
                            table.columns[target_idx].double_values.push_back(std::stod(field));
                        } catch (...) {
                            table.columns[target_idx].double_values.push_back(0.0);
                            table.columns[target_idx].null_mask.back() = 1;
                        }
                    } else if (table.columns[target_idx].type == "boolean") {
                        table.columns[target_idx].boolean_values.push_back(ParseBoolean(field) ? 1 : 0);
                    } else {
                        table.columns[target_idx].string_values.push_back(field);
                    }
                }
            }
            
            col_idx++;
            
            if (pos < end && *pos == dialect.delimiter) {
                pos++; // Skip delimiter
            } else if (pos < end && (*pos == '\n' || *pos == '\r')) {
                if (*pos == '\r' && pos + 1 < end && pos[1] == '\n') {
                    pos += 2;
                } else {
                    pos++;
                }
                break;
            } else if (pos >= end) {
                break;
            }
        }
        
        row_count++;
    }
    
    table.num_rows = row_count;
    table.success = true;
    
    return table;
}
