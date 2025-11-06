#include "decode.hpp"
#include "metadata.hpp"
#include "thrift.hpp"
#include "compression.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>

// Helper function to read LE integers from buffer
static inline int32_t ReadLE32(const uint8_t *p) {
  return (int32_t)p[0] | ((int32_t)p[1] << 8) | ((int32_t)p[2] << 16) |
         ((int32_t)p[3] << 24);
}

static inline int64_t ReadLE64(const uint8_t *p) {
  return (int64_t)p[0] | ((int64_t)p[1] << 8) | ((int64_t)p[2] << 16) |
         ((int64_t)p[3] << 24) | ((int64_t)p[4] << 32) | ((int64_t)p[5] << 40) |
         ((int64_t)p[6] << 48) | ((int64_t)p[7] << 56);
}

// Helper functions to read LE floats from buffer
static inline float ReadFloat32(const uint8_t *p) {
  uint32_t bits = ReadLE32(p);
  float value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

static inline double ReadFloat64(const uint8_t *p) {
  uint64_t bits = ReadLE64(p);
  double value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

// Skip RLE/Bit-packed encoded levels (repetition or definition levels)
// Returns the number of bytes to skip
// According to Parquet spec:
// - For Data Page V1, levels are RLE-encoded with 4-byte length prefix IF max_level > 0
// - If max_level = 0, NO level data is written at all
static size_t SkipRLEBitPackedLevels(const uint8_t* data, size_t max_size, int max_level) {
  if (max_level <= 0) {
    // No levels encoded when max_level = 0
    return 0;
  }
  
  // For max_level > 0, the level data is length-prefixed with a 4-byte little-endian int
  if (max_size < 4) {
    return 0;  // Not enough data
  }
  
  int32_t level_byte_length = ReadLE32(data);
  if (level_byte_length < 0 || level_byte_length > (int32_t)max_size - 4) {
    return 0;  // Invalid length
  }
  
  // Skip the 4-byte length + the level data itself
  return 4 + level_byte_length;
}

// Simple RLE/Bit-packed hybrid decoder for dictionary indices
// Returns the number of indices decoded, or -1 on error
// For simplicity, this implementation focuses on RLE runs which are most common in dictionary encoding
static int32_t DecodeRLEBitPackedIndices(const uint8_t* data, size_t data_size,
                                        int32_t num_values, int bit_width,
                                        std::vector<int32_t>& indices) {
  if (bit_width <= 0 || bit_width > 32 || data_size < 4) {
    return -1;
  }

  indices.clear();
  indices.reserve(num_values);

  // Skip 4-byte length prefix
  const uint8_t* ptr = data + 4;
  const uint8_t* end = data + data_size;

  int32_t decoded = 0;
  while (decoded < num_values && ptr < end) {
    // Read varint header
    uint32_t header = 0;
    int shift = 0;
    while (ptr < end && shift < 32) {
      uint8_t byte = *ptr++;
      header |= ((uint32_t)(byte & 0x7F)) << shift;
      if ((byte & 0x80) == 0) break;
      shift += 7;
    }
    
    if ((header & 1) == 0) {
      // Bit-packed run: (header >> 1) * 8 values
      int32_t num_groups = header >> 1;
      int32_t values_in_run = num_groups * 8;
      int32_t bytes_needed = (values_in_run * bit_width + 7) / 8;
      
      if (ptr + bytes_needed > end) break;
      
      // Decode bit-packed values
      for (int32_t i = 0; i < values_in_run && decoded < num_values; i++) {
        uint32_t value = 0;
        int bit_pos = i * bit_width;
        int byte_pos = bit_pos / 8;
        int bit_offset = bit_pos % 8;
        
        // Read up to 5 bytes to cover any bit_width up to 32
        for (int b = 0; b < 5 && byte_pos + b < bytes_needed; b++) {
          value |= ((uint32_t)ptr[byte_pos + b]) << (b * 8);
        }
        
        // Shift and mask to get the actual value
        value = (value >> bit_offset) & ((1U << bit_width) - 1);
        indices.push_back(value);
        decoded++;
      }
      
      ptr += bytes_needed;
      
    } else {
      // RLE run: (header >> 1) repetitions of next value
      int32_t count = header >> 1;
      
      // Read the value (bit_width bits, but always read full bytes)
      uint32_t value = 0;
      int bytes_needed = (bit_width + 7) / 8;
      for (int i = 0; i < bytes_needed && ptr < end; i++) {
        value |= ((uint32_t)(*ptr++)) << (i * 8);
      }
      
      // Mask to bit_width
      value &= (1U << bit_width) - 1;
      
      // Add 'count' copies of 'value'
      for (int32_t i = 0; i < count && decoded < num_values; i++) {
        indices.push_back(value);
        decoded++;
      }
    }
  }

  return decoded == num_values ? decoded : -1;
}

// Helper to read unsigned varint (zigzag encoded)
static int64_t ReadZigZagVarint(const uint8_t*& ptr, const uint8_t* end) {
  if (ptr >= end) return 0;
  
  uint64_t result = 0;
  int shift = 0;
  while (ptr < end && shift < 64) {
    uint8_t byte = *ptr++;
    result |= ((uint64_t)(byte & 0x7F)) << shift;
    if ((byte & 0x80) == 0) {
      // ZigZag decode: (n >>> 1) ^ -(n & 1)
      return (result >> 1) ^ -(result & 1);
    }
    shift += 7;
  }
  return 0;
}

// Helper to read unsigned varint
static uint64_t ReadUnsignedVarint(const uint8_t*& ptr, const uint8_t* end) {
  if (ptr >= end) return 0;
  
  uint64_t result = 0;
  int shift = 0;
  while (ptr < end && shift < 64) {
    uint8_t byte = *ptr++;
    result |= ((uint64_t)(byte & 0x7F)) << shift;
    if ((byte & 0x80) == 0) {
      return result;
    }
    shift += 7;
  }
  return 0;
}

// Decode DELTA_BINARY_PACKED encoding for int32/int64
// Returns number of values decoded, or -1 on error
template<typename T>
static int32_t DecodeDeltaBinaryPacked(const uint8_t* data, size_t data_size,
                                      int32_t num_values, std::vector<T>& values) {
  if (data_size < 4) return -1;
  
  const uint8_t* ptr = data;
  const uint8_t* end = data + data_size;
  
  values.clear();
  values.reserve(num_values);
  
  // Read header (once at the beginning)
  uint64_t block_size = ReadUnsignedVarint(ptr, end);
  if (block_size == 0 || ptr >= end) return -1;
  
  uint64_t num_miniblocks = ReadUnsignedVarint(ptr, end);
  if (num_miniblocks == 0 || ptr >= end) return -1;
  
  uint64_t total_value_count = ReadUnsignedVarint(ptr, end);
  if (ptr >= end) return -1;
  
  // Read and store first value
  int64_t first_value = ReadZigZagVarint(ptr, end);
  if (ptr > end) return -1;
  
  values.push_back(static_cast<T>(first_value));
  
  if (num_values == 1) {
    return 1;
  }
  
  int32_t decoded = 1;
  uint32_t values_per_miniblock = block_size / num_miniblocks;
  
  // Process blocks until we have all values
  while (decoded < num_values && ptr < end) {
    // Read min delta for this block
    int64_t min_delta = ReadZigZagVarint(ptr, end);
    if (ptr > end) break;
    
    // Read all bit widths for miniblocks in this block
    std::vector<uint8_t> bit_widths;
    bit_widths.reserve(num_miniblocks);
    for (uint64_t mb = 0; mb < num_miniblocks && ptr < end; mb++) {
      bit_widths.push_back(*ptr++);
    }
    
    if (bit_widths.size() != num_miniblocks) break;
    
    // Process each miniblock
    for (uint64_t mb = 0; mb < num_miniblocks && decoded < num_values; mb++) {
      uint8_t bit_width = bit_widths[mb];
      
      if (bit_width == 0) {
        // All values in this miniblock have delta = min_delta
        // No data bytes needed for bit_width=0
        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          T last = values.back();
          values.push_back(last + static_cast<T>(min_delta));
          decoded++;
        }
      } else {
        // Decode bit-packed deltas - need data bytes
        if (ptr >= end) break;
        
        int32_t bytes_needed = (values_per_miniblock * bit_width + 7) / 8;
        if (ptr + bytes_needed > end) break;
        
        for (uint32_t i = 0; i < values_per_miniblock && decoded < num_values; i++) {
          // Extract bit-packed value
          uint64_t delta = 0;
          int bit_pos = i * bit_width;
          int byte_pos = bit_pos / 8;
          int bit_offset = bit_pos % 8;
          
          // Read up to 9 bytes to cover any bit_width up to 64
          for (int b = 0; b < 9 && byte_pos + b < bytes_needed; b++) {
            delta |= ((uint64_t)ptr[byte_pos + b]) << (b * 8);
          }
          
          // Shift and mask to extract the value
          delta = (delta >> bit_offset) & ((1ULL << bit_width) - 1);
          
          // Apply delta: new_value = last_value + (min_delta + relative_delta)
          T last = values.back();
          values.push_back(last + static_cast<T>(min_delta + delta));
          decoded++;
        }
        
        ptr += bytes_needed;
      }
    }
  }
  
  return decoded;
}

// Helper function to skip DELTA_BINARY_PACKED data and return pointer after it
static const uint8_t* SkipDeltaBinaryPacked(const uint8_t* ptr, const uint8_t* end, int32_t num_values) {
  // Read header
  uint64_t block_size = ReadUnsignedVarint(ptr, end);
  if (block_size == 0 || ptr >= end) return ptr;
  
  uint64_t num_miniblocks = ReadUnsignedVarint(ptr, end);
  if (num_miniblocks == 0 || ptr >= end) return ptr;
  
  ReadUnsignedVarint(ptr, end);  // total_value_count
  if (ptr >= end) return ptr;
  
  ReadZigZagVarint(ptr, end);  // first_value
  if (ptr > end) return ptr;
  
  uint32_t values_per_miniblock = block_size / num_miniblocks;
  int32_t decoded = 1;
  
  // Skip blocks
  while (decoded < num_values && ptr < end) {
    ReadZigZagVarint(ptr, end);  // min_delta
    if (ptr > end) break;
    
    // Skip bit widths and miniblock data
    for (uint64_t mb = 0; mb < num_miniblocks && ptr < end; mb++) {
      uint8_t bit_width = *ptr++;
      if (bit_width > 0) {
        int32_t bytes_needed = (values_per_miniblock * bit_width + 7) / 8;
        if (ptr + bytes_needed > end) break;
        ptr += bytes_needed;
      }
      decoded += values_per_miniblock;
      if (decoded >= num_values) break;
    }
  }
  
  return ptr;
}

// Decode DELTA_BYTE_ARRAY encoding for byte arrays
// Returns number of values decoded, or -1 on error
static int32_t DecodeDeltaByteArray(const uint8_t* data, size_t data_size,
                                   int32_t num_values, std::vector<std::string>& values) {
  if (data_size < 4) return -1;
  
  const uint8_t* ptr = data;
  const uint8_t* end = data + data_size;
  
  values.clear();
  values.reserve(num_values);
  
  // Decode prefix lengths using DELTA_BINARY_PACKED
  std::vector<int32_t> prefix_lengths;
  int32_t prefix_decoded = DecodeDeltaBinaryPacked(ptr, end - ptr, num_values, prefix_lengths);
  
  if (prefix_decoded != num_values) {
    return -1;
  }
  
  // Skip past the prefix length data to find suffix lengths
  ptr = SkipDeltaBinaryPacked(data, end, num_values);
  if (ptr >= end) return -1;
  
  // Decode suffix lengths using DELTA_BINARY_PACKED
  const uint8_t* suffix_start = ptr;
  std::vector<int32_t> suffix_lengths;
  int32_t suffix_decoded = DecodeDeltaBinaryPacked(ptr, end - ptr, num_values, suffix_lengths);
  
  if (suffix_decoded != num_values) {
    return -1;
  }
  
  // Skip past suffix length data to find suffix strings
  ptr = SkipDeltaBinaryPacked(suffix_start, end, num_values);
  if (ptr > end) return -1;
  
  // Reconstruct strings from prefix lengths and suffixes
  std::string previous_value;
  for (int32_t i = 0; i < num_values; i++) {
    int32_t prefix_len = prefix_lengths[i];
    int32_t suffix_len = suffix_lengths[i];
    
    if (prefix_len < 0 || suffix_len < 0) {
      return -1;
    }
    
    if (ptr + suffix_len > end) {
      return -1;
    }
    
    // Build new string: prefix from previous + new suffix
    std::string value;
    if (prefix_len > 0) {
      if ((size_t)prefix_len > previous_value.size()) {
        return -1;
      }
      value = previous_value.substr(0, prefix_len);
    }
    
    if (suffix_len > 0) {
      value.append(reinterpret_cast<const char*>(ptr), suffix_len);
      ptr += suffix_len;
    }
    
    values.push_back(value);
    previous_value = value;
  }
  
  return values.size();
}

bool CanDecode(const std::string &path) {
  try {
    // Read metadata to check if we can decode this file
    FileStats metadata = ReadParquetMetadata(path);

    // Check all columns in all row groups
    for (const auto &rg : metadata.row_groups) {
      for (const auto &col : rg.columns) {
        // Check compression codec - support UNCOMPRESSED (0), SNAPPY (1), ZSTD (6)
        if (col.codec != 0 && col.codec != 1 && col.codec != 6) {
          return false;
        }

        // Check physical type - must be supported primitive types
        if (col.physical_type != "int32" && col.physical_type != "int64" &&
            col.physical_type != "byte_array" && col.physical_type != "boolean" &&
            col.physical_type != "float32" && col.physical_type != "float64") {
          return false;
        }

        // Check encodings - support PLAIN (0), PLAIN_DICTIONARY (2), DELTA_BINARY_PACKED (4), DELTA_BYTE_ARRAY (6), RLE_DICTIONARY (7)
        bool has_supported_encoding = false;
        for (int32_t enc : col.encodings) {
          if (enc == 0 || enc == 2 || enc == 4 || enc == 6 || enc == 7) {
            has_supported_encoding = true;
            break;
          }
        }
        if (!has_supported_encoding) {
          return false;
        }
      }
    }

    return true;
  } catch (...) {
    return false;
  }
}

bool CanDecode(const uint8_t* data, size_t size) {
  try {
    // Read metadata from memory buffer to check if we can decode this data
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);

    // Check all columns in all row groups
    for (const auto &rg : metadata.row_groups) {
      for (const auto &col : rg.columns) {
        // Check compression codec - support UNCOMPRESSED (0), SNAPPY (1), ZSTD (6)
        if (col.codec != 0 && col.codec != 1 && col.codec != 6) {
          return false;
        }

        // Check physical type - must be supported primitive types
        if (col.physical_type != "int32" && col.physical_type != "int64" &&
            col.physical_type != "byte_array" && col.physical_type != "boolean" &&
            col.physical_type != "float32" && col.physical_type != "float64") {
          return false;
        }

        // Check encodings - support PLAIN (0), PLAIN_DICTIONARY (2), DELTA_BINARY_PACKED (4), DELTA_BYTE_ARRAY (6), RLE_DICTIONARY (7)
        bool has_supported_encoding = false;
        for (int32_t enc : col.encodings) {
          if (enc == 0 || enc == 2 || enc == 4 || enc == 6 || enc == 7) {
            has_supported_encoding = true;
            break;
          }
        }
        if (!has_supported_encoding) {
          return false;
        }
      }
    }

    return true;
  } catch (...) {
    return false;
  }
}

// Parse a PageHeader to get page type, uncompressed size, value count, and encoding
struct PageHeader {
  int32_t page_type = -1;          // 0=DATA_PAGE, 1=INDEX_PAGE, 2=DICTIONARY_PAGE, etc.
  int32_t uncompressed_page_size = 0;
  int32_t compressed_page_size = 0;
  int32_t num_values = 0;
  int32_t encoding = 0;            // Encoding used (PLAIN=0, DELTA_BINARY_PACKED=4, DELTA_BYTE_ARRAY=6, etc.)
};

static PageHeader ParsePageHeader(TInput &in) {
  PageHeader header;
  int16_t last_id = 0;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: // type
      header.page_type = ReadI32(in);
      break;
    case 2: // uncompressed_page_size
      header.uncompressed_page_size = ReadI32(in);
      break;
    case 3: // compressed_page_size
      header.compressed_page_size = ReadI32(in);
      break;
    case 5: { // data_page_header (struct) - field type should be 12 for STRUCT
      int16_t dph_last_id = 0;
      while (true) {
        auto dph_fh = ReadFieldHeader(in, dph_last_id);
        if (dph_fh.type == 0)
          break;
        switch (dph_fh.id) {
        case 1: // num_values
          header.num_values = ReadI32(in);
          break;
        case 2: // encoding
          header.encoding = ReadI32(in);
          break;
        default:
          SkipField(in, dph_fh.type);
          break;
        }
      }
      break;
    }
    default:
      SkipField(in, fh.type);
      break;
    }
  }

  return header;
}

DecodedColumn DecodeColumn(const std::string &path,
                           const std::string &column_name,
                           const RowGroupStats &row_group, 
                           int row_group_index) {
  DecodedColumn result;

  try {
    // Find the column in the provided row group
    const ColumnStats *target_col = nullptr;

    for (const auto &col : row_group.columns) {
      if (col.name == column_name) {
        target_col = &col;
        break;
      }
    }

    if (!target_col) {
      return result;
    }

    // Check if we can decode this column's encoding
    bool has_supported_encoding = false;
    for (int32_t enc : target_col->encodings) {
      if (enc == 0) {  // PLAIN
        has_supported_encoding = true;
      } else if (enc == 4 || enc == 6 || enc == 2 || enc == 7 || enc == 8) {
        has_supported_encoding = true;
      }
    }
    if (!has_supported_encoding) {
      return result;
    }

    // Set the type
    result.type = target_col->physical_type;

    // Open the file and read the entire column chunk
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
      return result;
    }

    int64_t offset = target_col->data_page_offset;
    int64_t total_size = target_col->total_compressed_size;
    if (offset < 0 || total_size <= 0) {
      return result;
    }

    file.seekg(offset);

    // Read the entire column chunk
    std::vector<uint8_t> chunk_data(total_size);
    file.read(reinterpret_cast<char *>(chunk_data.data()), total_size);
    if (file.gcount() != total_size) {
      return result;
    }

    // Parse the page header to find where the data starts
    TInput header_in{chunk_data.data(), chunk_data.data() + chunk_data.size()};
    PageHeader page_header = ParsePageHeader(header_in);

    if (page_header.page_type != 0) {
      return result; // Not a data page
    }

    // Calculate how much of the buffer was used for the header
    size_t header_size = header_in.p - chunk_data.data();

    // Get the compressed page data
    // For compressed pages, use compressed_page_size from header
    // For uncompressed pages, compressed_page_size == uncompressed_page_size
    const uint8_t *compressed_data = chunk_data.data() + header_size;
    size_t compressed_size = page_header.compressed_page_size;
    
    // Fallback: if compressed_page_size is 0 or invalid, use remaining chunk
    if (compressed_size == 0 || compressed_size > chunk_data.size() - header_size) {
      compressed_size = chunk_data.size() - header_size;
    }

    // Decompress the page data if necessary
    std::vector<uint8_t> decompressed_data;
    const uint8_t *data_ptr;
    size_t data_size;
    
    if (target_col->codec == 0) {
      // UNCOMPRESSED - use data directly
      data_ptr = compressed_data;
      data_size = compressed_size;
    } else {
      // COMPRESSED - decompress first
      try {
        auto codec = rugo::compression::CodecFromInt(target_col->codec);
        decompressed_data = rugo::compression::DecompressData(
          compressed_data,
          compressed_size,
          page_header.uncompressed_page_size,
          codec
        );
        data_ptr = decompressed_data.data();
        data_size = decompressed_data.size();
      } catch (const std::exception& e) {
        // Decompression failed - return unsuccessful result
        return result;
      }
    }

    int32_t num_values = target_col->num_values;
    if (num_values <= 0) {
      num_values = page_header.num_values;
    }

    // Skip repetition and definition levels according to Parquet spec
    // For Data Page V1, both are RLE-encoded with 4-byte length prefixes when present
    // They are only present if max_level > 0
    
    // Skip repetition levels (only if max_repetition_level > 0)
    if (target_col->max_repetition_level > 0) {
      size_t rep_level_bytes = SkipRLEBitPackedLevels(data_ptr, data_size, target_col->max_repetition_level);
      data_ptr += rep_level_bytes;
      data_size -= rep_level_bytes;
    }
    
    // Skip definition levels (only if max_definition_level > 0)
    if (target_col->max_definition_level > 0) {
      size_t def_level_bytes = SkipRLEBitPackedLevels(data_ptr, data_size, target_col->max_definition_level);
      data_ptr += def_level_bytes;
      data_size -= def_level_bytes;
    }

    // Decode based on type and actual page encoding (from page header)
    const uint8_t *data_end = data_ptr + data_size;
    int32_t page_encoding = page_header.encoding;
    
    // DEBUG: Log encoding and type
    std::cerr << "DEBUG: Decoding column '" << column_name << "' type=" << result.type 
              << " encoding=" << page_encoding << " num_values=" << num_values 
              << " data_size=" << data_size << std::endl;
    
    if (result.type == "int32") {
      if (page_encoding == 4) {  // DELTA_BINARY_PACKED
        // Use DELTA_BINARY_PACKED decoder
        std::cerr << "DEBUG: Using DELTA_BINARY_PACKED for int32" << std::endl;
        int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, num_values, result.int32_values);
        std::cerr << "DEBUG: Decoded " << decoded << " values" << std::endl;
        result.success = (decoded == num_values);
      } else {
        // Use PLAIN encoding decoder
        result.int32_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
          int32_t value = ReadLE32(data_ptr);
          result.int32_values.push_back(value);
          data_ptr += 4;
        }
        result.success = (result.int32_values.size() == (size_t)num_values);
      }
    } else if (result.type == "int64") {
      if (page_encoding == 4) {  // DELTA_BINARY_PACKED
        // Use DELTA_BINARY_PACKED decoder
        std::cerr << "DEBUG: Using DELTA_BINARY_PACKED for int64" << std::endl;
        int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, num_values, result.int64_values);
        std::cerr << "DEBUG: Decoded " << decoded << " values" << std::endl;
        result.success = (decoded == num_values);
      } else {
        // Use PLAIN encoding decoder
        result.int64_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 8 <= data_end; i++) {
          int64_t value = ReadLE64(data_ptr);
          result.int64_values.push_back(value);
          data_ptr += 8;
        }
        result.success = (result.int64_values.size() == (size_t)num_values);
      }
    } else if (result.type == "byte_array") {
      if (page_encoding == 6) {  // DELTA_BYTE_ARRAY
        // Use DELTA_BYTE_ARRAY decoder
        std::cerr << "DEBUG: Using DELTA_BYTE_ARRAY for byte_array" << std::endl;
        int32_t decoded = DecodeDeltaByteArray(data_ptr, data_size, num_values, result.string_values);
        std::cerr << "DEBUG: Decoded " << decoded << " values" << std::endl;
        result.success = (decoded == num_values);
      } else {
        // PLAIN encoding for byte_array: each value is 4-byte length + data
        result.string_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
          int32_t length = ReadLE32(data_ptr);
          data_ptr += 4;

          if (data_ptr + length > data_end) {
            break;
          }

          std::string value(reinterpret_cast<const char *>(data_ptr), length);
          result.string_values.push_back(value);
          data_ptr += length;
        }
        result.success = (result.string_values.size() == (size_t)num_values);
      }
    } else if (result.type == "boolean") {
      // PLAIN encoding for boolean: 1 bit per value, packed into bytes
      result.boolean_values.reserve(num_values);
      for (int32_t i = 0; i < num_values && data_ptr < data_end; i++) {
        // Each byte contains up to 8 boolean values
        uint8_t byte_value = data_ptr[i / 8];
        uint8_t bit_value = (byte_value >> (i % 8)) & 1;
        result.boolean_values.push_back(bit_value);
        if ((i + 1) % 8 == 0) {
          data_ptr += 1;
        }
      }
      // Move past the last partial byte if necessary
      if (num_values % 8 != 0 && num_values > 0) {
        data_ptr += 1;
      }
      result.success = (result.boolean_values.size() == (size_t)num_values);
    } else if (result.type == "float32") {
      result.float32_values.reserve(num_values);
      for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
        float value = ReadFloat32(data_ptr);
        result.float32_values.push_back(value);
        data_ptr += 4;
      }
      result.success = (result.float32_values.size() == (size_t)num_values);
    } else if (result.type == "float64") {
      result.float64_values.reserve(num_values);
      for (int32_t i = 0; i < num_values && data_ptr + 8 <= data_end; i++) {
        double value = ReadFloat64(data_ptr);
        result.float64_values.push_back(value);
        data_ptr += 8;
      }
      result.success = (result.float64_values.size() == (size_t)num_values);
    }

  } catch (...) {
    result.success = false;
  }

  return result;
}

// Helper function to decode column data from a memory buffer
static DecodedColumn DecodeColumnFromChunk(const uint8_t* file_data, size_t file_size,
                                          const ColumnStats* target_col) {
  DecodedColumn result;
  
  std::cerr << "DEBUG: DecodeColumnFromChunk data_offset=" << target_col->data_page_offset
            << " total_compressed_size=" << target_col->total_compressed_size
            << " codec=" << target_col->codec << " type=" << target_col->physical_type << std::endl;
  
  try {
    // Check if we can decode this column (updated to support compression)
    if (target_col->codec != 0 && target_col->codec != 1 && target_col->codec != 6) {
      std::cerr << "DEBUG: Unsupported codec " << target_col->codec << std::endl;
      return result; // Unsupported compression codec
    }

    bool has_supported_encoding = false;
    for (int32_t enc : target_col->encodings) {
      if (enc == 0) {  // PLAIN
        has_supported_encoding = true;
      } else if (enc == 4) {  // DELTA_BINARY_PACKED
        has_supported_encoding = true;
      } else if (enc == 6) {  // DELTA_BYTE_ARRAY
        has_supported_encoding = true;
      } else if (enc == 2 || enc == 7 || enc == 8) {  // Dictionary encodings
        has_supported_encoding = true;
      }
    }
    if (!has_supported_encoding) {
      std::cerr << "DEBUG: No supported encoding found in column encodings" << std::endl;
      return result; // No supported encoding
    }

    // Set the type
    result.type = target_col->physical_type;

    // Dictionary to store values if dictionary encoding is used
    std::vector<int32_t> dict_int32;
    std::vector<int64_t> dict_int64;
    std::vector<std::string> dict_string;
    std::vector<float> dict_float32;
    std::vector<double> dict_float64;
    int32_t dict_size = 0;
    
    // Track decompressed data to keep it in scope
    std::vector<uint8_t> dict_decompressed_data;
    std::vector<uint8_t> page_decompressed_data;

    // Attempt to load dictionary page if offset is provided
    std::cerr << "DEBUG: Checking for dictionary page, offset=" << target_col->dictionary_page_offset << std::endl;

    if (target_col->dictionary_page_offset >= 0 &&
        (uint64_t)target_col->dictionary_page_offset < file_size) {
      const uint8_t* dict_ptr = file_data + target_col->dictionary_page_offset;
      const uint8_t* dict_end_ptr = file_data + file_size;

      TInput dict_header_in{dict_ptr, dict_end_ptr};
      PageHeader dict_page_header = ParsePageHeader(dict_header_in);
      std::cerr << "DEBUG: Dictionary header parsed, type=" << dict_page_header.page_type << std::endl;

      if (dict_page_header.page_type == 2) {  // DICTIONARY_PAGE
        size_t dict_header_size = dict_header_in.p - dict_ptr;
        size_t dict_compressed_size = dict_page_header.compressed_page_size;

        if (dict_header_size > (size_t)(dict_end_ptr - dict_ptr)) {
          return result;
        }

        const uint8_t* dict_compressed_data = dict_ptr + dict_header_size;
        if (dict_compressed_size == 0 || dict_compressed_size > (size_t)(dict_end_ptr - dict_compressed_data)) {
          dict_compressed_size = dict_end_ptr - dict_compressed_data;
        }

        const uint8_t* dict_data_ptr;
        size_t dict_data_size;

        if (target_col->codec == 0) {
          dict_data_ptr = dict_compressed_data;
          dict_data_size = dict_compressed_size;
        } else {
          try {
            auto codec = rugo::compression::CodecFromInt(target_col->codec);
            dict_decompressed_data = rugo::compression::DecompressData(
              dict_compressed_data,
              dict_compressed_size,
              dict_page_header.uncompressed_page_size,
              codec
            );
            dict_data_ptr = dict_decompressed_data.data();
            dict_data_size = dict_decompressed_data.size();
          } catch (...) {
            return result;
          }
        }

        dict_size = dict_page_header.num_values;
        const uint8_t* dict_end = dict_data_ptr + dict_data_size;

        if (result.type == "int32") {
          dict_int32.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            dict_int32.push_back(ReadLE32(dict_data_ptr));
            dict_data_ptr += 4;
          }
        } else if (result.type == "int64") {
          dict_int64.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 8 <= dict_end; i++) {
            dict_int64.push_back(ReadLE64(dict_data_ptr));
            dict_data_ptr += 8;
          }
        } else if (result.type == "byte_array") {
          dict_string.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            int32_t length = ReadLE32(dict_data_ptr);
            dict_data_ptr += 4;
            if (dict_data_ptr + length > dict_end) break;
            dict_string.emplace_back(reinterpret_cast<const char*>(dict_data_ptr), length);
            dict_data_ptr += length;
          }
        } else if (result.type == "float32") {
          dict_float32.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 4 <= dict_end; i++) {
            dict_float32.push_back(ReadFloat32(dict_data_ptr));
            dict_data_ptr += 4;
          }
        } else if (result.type == "float64") {
          dict_float64.reserve(dict_size);
          for (int32_t i = 0; i < dict_size && dict_data_ptr + 8 <= dict_end; i++) {
            dict_float64.push_back(ReadFloat64(dict_data_ptr));
            dict_data_ptr += 8;
          }
        }
      }
    }

    // Prepare to read the actual data pages starting at data_page_offset
    if (target_col->data_page_offset < 0 ||
        (uint64_t)target_col->data_page_offset >= file_size) {
      return result;
    }

    const uint8_t* current_ptr = file_data + target_col->data_page_offset;
    size_t remaining_size = 0;
    if (target_col->total_compressed_size > 0) {
      uint64_t max_size = file_size - target_col->data_page_offset;
      remaining_size = std::min<uint64_t>(target_col->total_compressed_size, max_size);
    } else {
      remaining_size = file_size - target_col->data_page_offset;
    }

    // Parse the data page header
    std::cerr << "DEBUG: About to parse page header, remaining_size=" << remaining_size << std::endl;
    TInput header_in{current_ptr, current_ptr + remaining_size};
    std::cerr << "DEBUG: TInput created" << std::endl;
    PageHeader page_header = ParsePageHeader(header_in);
    std::cerr << "DEBUG: Page header parsed, type=" << page_header.page_type << std::endl;

    if (page_header.page_type != 0) {
      std::cerr << "DEBUG: Not a data page, returning" << std::endl;
      return result; // Not a data page
    }

    // Calculate how much of the buffer was used for the header
    size_t header_size = header_in.p - current_ptr;

    // Get the compressed page data
    // For compressed pages, use compressed_page_size from header
    // For uncompressed pages, compressed_page_size == uncompressed_page_size
    const uint8_t *compressed_data = current_ptr + header_size;
    size_t compressed_size = page_header.compressed_page_size;
    
    // Fallback: if compressed_page_size is 0 or invalid, use remaining chunk
    if (compressed_size == 0 || compressed_size > remaining_size - header_size) {
      compressed_size = remaining_size - header_size;
    }

    // Decompress the page data if necessary
    const uint8_t *data_ptr;
    size_t data_size;
    
    if (target_col->codec == 0) {
      // UNCOMPRESSED - use data directly
      data_ptr = compressed_data;
      data_size = compressed_size;
    } else {
      // COMPRESSED - decompress first
      try {
        std::cerr << "DEBUG: About to decompress, codec=" << target_col->codec 
                  << " compressed_size=" << compressed_size 
                  << " uncompressed_page_size=" << page_header.uncompressed_page_size << std::endl;
        auto codec = rugo::compression::CodecFromInt(target_col->codec);
        std::cerr << "DEBUG: Codec created" << std::endl;
        page_decompressed_data = rugo::compression::DecompressData(
          compressed_data,
          compressed_size,
          page_header.uncompressed_page_size,
          codec
        );
        std::cerr << "DEBUG: Decompression complete, decompressed size=" << page_decompressed_data.size() << std::endl;
        data_ptr = page_decompressed_data.data();
        data_size = page_decompressed_data.size();
      } catch (const std::exception& e) {
        // Decompression failed - return unsuccessful result
        std::cerr << "DEBUG: Decompression exception: " << e.what() << std::endl;
        return result;
      }
    }

    int32_t num_values = target_col->num_values;
    if (num_values <= 0) {
      num_values = page_header.num_values;
    }

    // Skip repetition and definition levels according to Parquet spec
    // For Data Page V1, both are RLE-encoded with 4-byte length prefixes when present
    // They are only present if max_level > 0
    
    std::cerr << "DEBUG: Before skipping levels, data_size=" << data_size 
              << " max_rep_level=" << target_col->max_repetition_level 
              << " max_def_level=" << target_col->max_definition_level << std::endl;
    
    // Skip repetition levels (only if max_repetition_level > 0)
    if (target_col->max_repetition_level > 0) {
      size_t rep_level_bytes = SkipRLEBitPackedLevels(data_ptr, data_size, target_col->max_repetition_level);
      std::cerr << "DEBUG: Skipped " << rep_level_bytes << " rep level bytes" << std::endl;
      data_ptr += rep_level_bytes;
      data_size -= rep_level_bytes;
    }
    
    // Skip definition levels (only if max_definition_level > 0)
    if (target_col->max_definition_level > 0) {
      size_t def_level_bytes = SkipRLEBitPackedLevels(data_ptr, data_size, target_col->max_definition_level);
      std::cerr << "DEBUG: Skipped " << def_level_bytes << " def level bytes" << std::endl;
      data_ptr += def_level_bytes;
      data_size -= def_level_bytes;
    }
    
    std::cerr << "DEBUG: After skipping levels, data_size=" << data_size 
              << " num_values=" << num_values 
              << " page_encoding=" << page_header.encoding << std::endl;

    // Decode based on type and encoding
    const uint8_t *data_end = data_ptr + data_size;
    
    bool encoding_requires_dictionary = (page_header.encoding == 2 || page_header.encoding == 8);
    bool page_uses_dictionary = encoding_requires_dictionary && dict_size > 0;

    if (encoding_requires_dictionary && dict_size == 0) {
      std::cerr << "DEBUG: Dictionary encoding requested but dictionary not loaded" << std::endl;
      return result;
    }

    if (page_uses_dictionary) {
      std::cerr << "DEBUG: Using dictionary encoding, dict_size=" << dict_size << std::endl;
      // Dictionary encoding: decode indices then map to dictionary values
      // Calculate bit width for indices
      int bit_width = 0;
      int32_t max_index = dict_size - 1;
      while (max_index > 0) {
        bit_width++;
        max_index >>= 1;
      }
      if (bit_width == 0) bit_width = 1;
      
      std::cerr << "DEBUG: Bit width for indices=" << bit_width << std::endl;
      
      std::vector<int32_t> indices;
      int32_t decoded = DecodeRLEBitPackedIndices(data_ptr, data_size, num_values, bit_width, indices);
      
      std::cerr << "DEBUG: Decoded " << decoded << " indices" << std::endl;
      
      if (decoded != num_values) {
        return result; // Failed to decode indices
      }
      
      // Map indices to dictionary values
      if (result.type == "int32") {
        result.int32_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx >= 0 && idx < (int32_t)dict_int32.size()) {
            result.int32_values.push_back(dict_int32[idx]);
          } else {
            return result; // Invalid index
          }
        }
        result.success = (result.int32_values.size() == (size_t)num_values);
      } else if (result.type == "int64") {
        result.int64_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx >= 0 && idx < (int32_t)dict_int64.size()) {
            result.int64_values.push_back(dict_int64[idx]);
          } else {
            return result;
          }
        }
        result.success = (result.int64_values.size() == (size_t)num_values);
      } else if (result.type == "byte_array") {
        result.string_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx >= 0 && idx < (int32_t)dict_string.size()) {
            result.string_values.push_back(dict_string[idx]);
          } else {
            return result;
          }
        }
        result.success = (result.string_values.size() == (size_t)num_values);
      } else if (result.type == "float32") {
        result.float32_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx >= 0 && idx < (int32_t)dict_float32.size()) {
            result.float32_values.push_back(dict_float32[idx]);
          } else {
            return result;
          }
        }
        result.success = (result.float32_values.size() == (size_t)num_values);
      } else if (result.type == "float64") {
        result.float64_values.reserve(num_values);
        for (int32_t idx : indices) {
          if (idx >= 0 && idx < (int32_t)dict_float64.size()) {
            result.float64_values.push_back(dict_float64[idx]);
          } else {
            return result;
          }
        }
        result.success = (result.float64_values.size() == (size_t)num_values);
      }
      
    } else {
      // PLAIN or DELTA encoding (use actual page encoding from header)
      int32_t page_encoding = page_header.encoding;
      
      if (result.type == "int32") {
        if (page_encoding == 4) {  // DELTA_BINARY_PACKED
          // Use DELTA_BINARY_PACKED decoder
          int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, num_values, result.int32_values);
          result.success = (decoded == num_values);
        } else {
          // PLAIN encoding
          result.int32_values.reserve(num_values);
          for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
            int32_t value = ReadLE32(data_ptr);
            result.int32_values.push_back(value);
            data_ptr += 4;
          }
          result.success = (result.int32_values.size() == (size_t)num_values);
        }
      } else if (result.type == "int64") {
        if (page_encoding == 4) {  // DELTA_BINARY_PACKED
          // Use DELTA_BINARY_PACKED decoder
          int32_t decoded = DecodeDeltaBinaryPacked(data_ptr, data_size, num_values, result.int64_values);
          result.success = (decoded == num_values);
        } else {
          // PLAIN encoding
          result.int64_values.reserve(num_values);
          for (int32_t i = 0; i < num_values && data_ptr + 8 <= data_end; i++) {
            int64_t value = ReadLE64(data_ptr);
            result.int64_values.push_back(value);
            data_ptr += 8;
          }
          result.success = (result.int64_values.size() == (size_t)num_values);
        }
      } else if (result.type == "byte_array") {
        if (page_encoding == 6) {  // DELTA_BYTE_ARRAY
          // Use DELTA_BYTE_ARRAY decoder
          int32_t decoded = DecodeDeltaByteArray(data_ptr, data_size, num_values, result.string_values);
          result.success = (decoded == num_values);
        } else {
          // PLAIN encoding for byte_array: each value is 4-byte length + data
          result.string_values.reserve(num_values);
          for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
            int32_t length = ReadLE32(data_ptr);
            data_ptr += 4;

            if (data_ptr + length > data_end) {
              break;
            }

            std::string value(reinterpret_cast<const char *>(data_ptr), length);
            result.string_values.push_back(value);
            data_ptr += length;
          }
          result.success = (result.string_values.size() == (size_t)num_values);
        }
      } else if (result.type == "boolean") {
        // PLAIN encoding for boolean: 1 bit per value, packed into bytes
        result.boolean_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr < data_end; i++) {
          // Each byte contains up to 8 boolean values
          uint8_t byte_value = data_ptr[i / 8];
          uint8_t bit_value = (byte_value >> (i % 8)) & 1;
          result.boolean_values.push_back(bit_value);
          if ((i + 1) % 8 == 0) {
            data_ptr += 1;
          }
        }
        // Move past the last partial byte if necessary
        if (num_values % 8 != 0 && num_values > 0) {
          data_ptr += 1;
        }
        result.success = (result.boolean_values.size() == (size_t)num_values);
      } else if (result.type == "float32") {
        result.float32_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 4 <= data_end; i++) {
          float value = ReadFloat32(data_ptr);
          result.float32_values.push_back(value);
          data_ptr += 4;
        }
        result.success = (result.float32_values.size() == (size_t)num_values);
      } else if (result.type == "float64") {
        result.float64_values.reserve(num_values);
        for (int32_t i = 0; i < num_values && data_ptr + 8 <= data_end; i++) {
          double value = ReadFloat64(data_ptr);
          result.float64_values.push_back(value);
          data_ptr += 8;
        }
        result.success = (result.float64_values.size() == (size_t)num_values);
      }
    }

  } catch (const std::exception& e) {
    std::cerr << "DEBUG: DecodeColumnFromChunk caught std::exception: " << e.what() << std::endl;
    result.success = false;
  } catch (...) {
    std::cerr << "DEBUG: DecodeColumnFromChunk caught unknown exception" << std::endl;
    result.success = false;
  }
  
  std::cerr << "DEBUG: DecodeColumnFromChunk returning, success=" << result.success 
            << " type=" << result.type << std::endl;

  return result;
}

// Decode a specific column from memory buffer for a specific row group
DecodedColumn DecodeColumnFromMemory(const uint8_t* data, size_t size, 
                                   const std::string &column_name,
                                   const RowGroupStats &row_group, 
                                   int row_group_index) {
  DecodedColumn result;
  
  std::cerr << "DEBUG: DecodeColumnFromMemory called for column '" << column_name 
            << "' row_group=" << row_group_index << std::endl;

  try {
    // Find the column in the provided row group
    const ColumnStats *target_col = nullptr;

    for (const auto &col : row_group.columns) {
      if (col.name == column_name) {
        target_col = &col;
        break;
      }
    }

    if (!target_col) {
      std::cerr << "DEBUG: Column '" << column_name << "' not found in row group" << std::endl;
      return result;
    }
    
    std::cerr << "DEBUG: Found column, offset=" << target_col->data_page_offset 
              << " size=" << target_col->total_compressed_size << std::endl;

    int64_t offset = target_col->data_page_offset;
    int64_t total_size = target_col->total_compressed_size;
    if (offset < 0 || total_size <= 0) {
      std::cerr << "DEBUG: Invalid offset or size" << std::endl;
      return result;
    }

    // Check bounds
    if (offset + total_size > (int64_t)size) {
      return result;
    }

  return DecodeColumnFromChunk(data, size, target_col);

  } catch (...) {
    result.success = false;
  }

  return result;
}

// NEW PRIMARY API: Read parquet data from memory view with column selection
DecodedTable ReadParquet(const uint8_t* data, size_t size, const std::vector<std::string>& column_names) {
  DecodedTable table;
  
  try {
    // Read metadata from the memory buffer
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);
    
    // Set up the table structure
    table.column_names = column_names;
    table.row_groups.resize(metadata.row_groups.size());
    
    // Process each row group
    for (size_t rg_idx = 0; rg_idx < metadata.row_groups.size(); rg_idx++) {
      const RowGroupStats& row_group = metadata.row_groups[rg_idx];
      table.row_groups[rg_idx].resize(column_names.size());
      
      // Decode each requested column
      for (size_t col_idx = 0; col_idx < column_names.size(); col_idx++) {
        const std::string& column_name = column_names[col_idx];
        
        table.row_groups[rg_idx][col_idx] = DecodeColumnFromMemory(
          data, size, column_name, row_group, rg_idx
        );
      }
    }
    
    table.success = true;
    
  } catch (...) {
    table.success = false;
  }
  
  return table;
}

// Overload that decodes all columns when none are specified
DecodedTable ReadParquet(const uint8_t* data, size_t size) {
  DecodedTable table;
  
  try {
    // Read metadata from the memory buffer
    FileStats metadata = ReadParquetMetadataFromBuffer(data, size);
    
    // Extract all column names from the first row group
    std::vector<std::string> all_column_names;
    if (!metadata.row_groups.empty()) {
      for (const auto& col : metadata.row_groups[0].columns) {
        all_column_names.push_back(col.name);
      }
    }
    
    // Use the existing function with all column names
    return ReadParquet(data, size, all_column_names);
    
  } catch (...) {
    table.success = false;
  }
  
  return table;
}

// Backward compatibility overload - reads metadata and decodes from first row group
DecodedColumn DecodeColumn(const std::string &path, const std::string &column_name) {
  try {
    FileStats metadata = ReadParquetMetadata(path);
    if (metadata.row_groups.empty()) {
      return DecodedColumn{};
    }
    return DecodeColumn(path, column_name, metadata.row_groups[0], 0);
  } catch (...) {
    return DecodedColumn{};
  }
}
