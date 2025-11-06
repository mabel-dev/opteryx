#include "metadata.hpp"
#include "thrift.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <functional>
#include <stdexcept>
#include <vector>

// ------------------- Helpers -------------------

static inline uint32_t ReadLE32(const uint8_t *p) {
  return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static inline const char *ParquetTypeToString(int t) {
  switch (t) {
  case 0:
    return "boolean";
  case 1:
    return "int32";
  case 2:
    return "int64";
  case 3:
    return "int96";
  case 4:
    return "float32";
  case 5:
    return "float64";
  case 6:
    return "byte_array";
  case 7:
    return "fixed_len_byte_array";
  default:
    return "unknown";
  }
}

static inline const char *LogicalTypeToString(int t) {
  switch (t) {
  case 0:
    return "varchar"; // UTF8
  case 1:
    return "MAP";
  case 2:
    return "LIST";
  case 3:
    return "ENUM";
  case 4:
    return "DECIMAL";
  case 5:
    return "DATE";
  case 6:
    return "TIME_MILLIS";
  case 7:
    return "TIME_MICROS";
  case 8:
    return "TIMESTAMP_MILLIS";
  case 9:
    return "TIMESTAMP_MICROS";
  case 10:
    return "UINT_8";
  case 11:
    return "UINT_16";
  case 12:
    return "UINT_32";
  case 13:
    return "UINT_64";
  case 14:
    return "INT_8";
  case 15:
    return "INT_16";
  case 16:
    return "INT_32";
  case 17:
    return "INT_64";
  case 18:
    return "JSON";
  case 19:
    return "BSON";
  case 20:
    return "INTERVAL";
  case 21:
    return "struct";
  default:
    return "";
  }
}

const char *EncodingToString(int32_t enc) {
  switch (enc) {
  case 0:
    return "PLAIN";
  case 1:
    return "PLAIN_DICTIONARY";
  case 2:
    return "RLE";
  case 3:
    return "BIT_PACKED";
  case 4:
    return "DELTA_BINARY_PACKED";
  case 5:
    return "DELTA_LENGTH_BYTE_ARRAY";
  case 6:
    return "DELTA_BYTE_ARRAY";
  case 7:
    return "RLE_DICTIONARY";
  case 8:
    return "BYTE_STREAM_SPLIT";
  default:
    return "UNKNOWN";
  }
}

const char *CompressionCodecToString(int32_t codec) {
  switch (codec) {
  case 0:
    return "UNCOMPRESSED";
  case 1:
    return "SNAPPY";
  case 2:
    return "GZIP";
  case 3:
    return "LZO";
  case 4:
    return "BROTLI";
  case 5:
    return "LZ4";
  case 6:
    return "ZSTD";
  case 7:
    return "LZ4_RAW";
  default:
    return "UNKNOWN";
  }
}

static inline std::string CanonicalizeColumnName(std::string name) {
  if (name.rfind("schema.", 0) == 0) {
    name.erase(0, 7); // strip schema.
  }
  if (name.size() >= 13 &&
      name.compare(name.size() - 13, 13, ".list.element") == 0) {
    name.erase(name.size() - 13);
  } else if (name.size() >= 10 &&
             name.compare(name.size() - 10, 10, ".list.item") == 0) {
    name.erase(name.size() - 10);
  }
  return name;
}

// ------------------- Schema parsing -------------------

// Correct logical type structure parsing
static std::string ParseLogicalType(TInput &in) {
  std::string result;
  int16_t last_id = 0;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: {             // STRING (StringType - empty struct)
      SkipStruct(in);     // Just skip the empty StringType struct
      result = "varchar"; // Use varchar for STRING type
      break;
    }
    case 2: { // MAP (MapType - empty struct)
      SkipStruct(in);
      result = "map";
      break;
    }
    case 3: { // LIST (ListType - empty struct)
      SkipStruct(in);
      result = "array";
      break;
    }
    case 4: { // ENUM (EnumType - empty struct)
      SkipStruct(in);
      result = "enum";
      break;
    }
    case 5: { // DECIMAL (DecimalType)
      int32_t scale = 0, precision = 0;
      int16_t decimal_last = 0;
      while (true) {
        auto inner = ReadFieldHeader(in, decimal_last);
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          scale = ReadI32(in);
        else if (inner.id == 2)
          precision = ReadI32(in);
        else
          SkipField(in, inner.type);
      }
      result = "decimal(" + std::to_string(precision) + "," +
               std::to_string(scale) + ")";
      break;
    }
    case 6: { // DATE (DateType - empty struct)
      SkipStruct(in);
      result = "date32[day]";
      break;
    }
    case 7: { // TIME (TimeType)
      int16_t time_last = 0;
      bool isAdjustedToUTC = false;
      std::string unit = "ms";
      while (true) {
        auto inner = ReadFieldHeader(in, time_last);
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          isAdjustedToUTC = (ReadBool(in) != 0);
        else if (inner.id == 2) { // unit
          int16_t unit_last = 0;
          while (true) {
            auto unit_fh = ReadFieldHeader(in, unit_last);
            if (unit_fh.type == 0)
              break;
            if (unit_fh.id == 1) { // MILLISECONDS
              SkipStruct(in);
              unit = "ms";
            } else if (unit_fh.id == 2) { // MICROSECONDS
              SkipStruct(in);
              unit = "us";
            } else if (unit_fh.id == 3) { // NANOSECONDS
              SkipStruct(in);
              unit = "ns";
            } else {
              SkipField(in, unit_fh.type);
            }
          }
        } else {
          SkipField(in, inner.type);
        }
      }
      result = "time[" + unit + (isAdjustedToUTC ? ",UTC" : "") + "]";
      SkipStruct(in);
      break;
    }
    case 8: { // TIMESTAMP (TimestampType)
      int16_t ts_last = 0;
      bool isAdjustedToUTC = false;
      std::string unit = "ms";
      while (true) {
        auto inner = ReadFieldHeader(in, ts_last);
        if (inner.type == 0)
          break;
        if (inner.id == 1)
          isAdjustedToUTC = (ReadBool(in) != 0);
        else if (inner.id == 2) { // unit
          int16_t unit_last = 0;
          while (true) {
            auto unit_fh = ReadFieldHeader(in, unit_last);
            if (unit_fh.type == 0)
              break;
            if (unit_fh.id == 1) { // MILLISECONDS
              SkipStruct(in);
              unit = "ms";
            } else if (unit_fh.id == 2) { // MICROSECONDS
              SkipStruct(in);
              unit = "us";
            } else if (unit_fh.id == 3) { // NANOSECONDS
              SkipStruct(in);
              unit = "ns";
            } else {
              SkipField(in, unit_fh.type);
            }
          }
        } else {
          SkipField(in, inner.type);
        }
      }
      result = "timestamp[" + unit + (isAdjustedToUTC ? ",UTC" : "") + "]";
      SkipStruct(in);
      break;
    }
    case 10: { // INTEGER (IntType)
      int16_t int_last = 0;
      int8_t bitWidth = 0;
      bool isSigned = true;

      while (true) {
        auto inner = ReadFieldHeader(in, int_last);
        if (inner.type == 0)
          break; // STOP

        if (inner.id == 1) {
          // bitWidth is just a single byte
          bitWidth = static_cast<int8_t>(in.readByte());
        } else if (inner.id == 2) {
          if (inner.type == T_BOOL_TRUE) {
            isSigned = true;
          } else if (inner.type == T_BOOL_FALSE) {
            isSigned = false;
          } else {
            isSigned = ReadBool(in);
          }
        } else {
          SkipField(in, inner.type); // future-proof
        }
      }

      result = (isSigned ? "int" : "uint") + std::to_string((int)bitWidth);
      break;
    }
    case 11: { // UNKNOWN (NullType - empty)
      SkipStruct(in);
      result = "unknown";
      break;
    }
    case 12: { // JSON (JsonType - empty)
      SkipStruct(in);
      result = "json";
      break;
    }
    case 13: { // BSON (BsonType - empty)
      SkipStruct(in);
      result = "bson";
      break;
    }
    case 15: {        // FLOAT16 (Float16Type - empty struct)
      SkipStruct(in); // it’s defined as an empty struct
      result = "float16";
      break;
    }
    default:
      std::cerr << "Skipping unknown logical type id " << fh.id << " type "
                << (int)fh.type << "\n";
      SkipField(in, fh.type);
      break;
    }
  }

  return result;
}

// Parse a SchemaElement
static SchemaElement ParseSchemaElement(TInput &in) {
  SchemaElement elem;
  int16_t last_id = 0;
  bool saw_physical_type = false;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: { // type (Physical type)
      int32_t t = ReadI32(in);
      saw_physical_type = true;
      elem.physical_type = ParquetTypeToString(t);
      break;
    }
    case 2: { // type_length (for FIXED_LEN_BYTE_ARRAY)
      int32_t len = ReadI32(in);
      elem.type_length = len;
      break;
    }
    case 3: { // repetition_type
      elem.repetition_type = ReadI32(in);
      break;
    }
    case 4: { // name
      elem.name = ReadString(in);
      break;
    }
    case 5: { // num_children
      elem.num_children = ReadI32(in);
      break;
    }
    case 6: { // converted_type (legacy logical type)
      int32_t ct = ReadI32(in);
      if (elem.logical_type.empty()) {
        elem.logical_type = LogicalTypeToString(ct);
      }
      break;
    }
    case 7: { // scale (for DECIMAL)
      int32_t scale = ReadI32(in);
      elem.scale = scale;
      break;
    }
    case 8: { // precision (for DECIMAL)
      int32_t precision = ReadI32(in);
      elem.precision = precision;
      break;
    }
    case 9: { // field_id
      int32_t field_id = ReadI32(in);
      (void)field_id;
      break;
    }
    case 10: { // logicalType (newer format)
      std::string logical = ParseLogicalType(in);
      if (!logical.empty()) {
        elem.logical_type = logical;
      }
      break;
    }
    default:
      SkipField(in, fh.type);
      break;
    }
  }

  // Detect struct nodes: no physical type, has children, no logical_type
  if (elem.num_children > 0 && !saw_physical_type &&
      elem.logical_type.empty()) {
    elem.logical_type = "struct";
  }

  return elem;
}

// ------------------- Parsers -------------------

// parquet.thrift Statistics
// 1: optional binary max
// 2: optional binary min
// 3: optional i64 null_count
// 4: optional i64 distinct_count
// 5: optional binary max_value
// 6: optional binary min_value
static void ParseStatistics(TInput &in, ColumnStats &cs) {
  std::string legacy_min, legacy_max, v2_min, v2_max;
  bool legacy_min_set = false;
  bool legacy_max_set = false;
  bool v2_min_set = false;
  bool v2_max_set = false;
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    switch (fh.id) {
    case 1:
      legacy_max = ReadString(in);
      legacy_max_set = true;
      break;
    case 2:
      legacy_min = ReadString(in);
      legacy_min_set = true;
      break;
    case 3:
      cs.null_count = ReadI64(in);
      break;
    case 4:
      cs.distinct_count = ReadI64(in);
      break;
    case 5:
      v2_max = ReadString(in);
      v2_max_set = true;
      break;
    case 6:
      v2_min = ReadString(in);
      v2_min_set = true;
      break;
    default:
      SkipField(in, fh.type);
      break;
    }
  }
  if (v2_min_set) {
    cs.min = v2_min;
    cs.has_min = true;
  } else if (legacy_min_set) {
    cs.min = legacy_min;
    cs.has_min = true;
  } else {
    cs.min.clear();
    cs.has_min = false;
  }

  if (v2_max_set) {
    cs.max = v2_max;
    cs.has_max = true;
  } else if (legacy_max_set) {
    cs.max = legacy_max;
    cs.has_max = true;
  } else {
    cs.max.clear();
    cs.has_max = false;
  }
}

// parquet.thrift ColumnMetaData
//  1: required Type type
//  2: required list<Encoding> encodings
//  3: required list<string> path_in_schema
//  4: required CompressionCodec codec
//  5: required i64 num_values
//  6: required i64 total_uncompressed_size
//  7: required i64 total_compressed_size
//  8: optional KeyValueMetaData key_value_metadata
//  9: optional i64 data_page_offset
// 10: optional i64 index_page_offset
// 11: optional i64 dictionary_page_offset
// 12: optional Statistics statistics
// 13: optional list<PageEncodingStats> encoding_stats
// 14+: later additions; Bloom filter fields are commonly (per spec updates):
//      14: optional i64 bloom_filter_offset
//      15: optional i64 bloom_filter_length
static void ParseColumnMeta(TInput &in, ColumnStats &cs,
                            const MetadataParseOptions &opts) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: {
      int32_t t = ReadI32(in);
      cs.physical_type = ParquetTypeToString(t);
      break;
    }
    case 2: { // encodings
      auto lh = ReadListHeader(in);
      for (uint32_t i = 0; i < lh.size; i++) {
        int32_t enc = ReadVarint(in);
        cs.encodings.push_back(enc);
      }
      break;
    }
    case 3: {
      auto lh = ReadListHeader(in);
      std::string name;
      for (uint32_t i = 0; i < lh.size; i++) {
        std::string part = ReadString(in);
        if (!name.empty())
          name.push_back('.');
        name += part;
      }
      cs.name = CanonicalizeColumnName(std::move(name));
      break;
    }
    case 4: {
      cs.codec = ReadI32(in);
      break;
    }
    case 5: {
      cs.num_values = ReadI64(in);
      break;
    }
    case 6: {
      cs.total_uncompressed_size = ReadI64(in);
      break;
    }
    case 7: {
      cs.total_compressed_size = ReadI64(in);
      break;
    }
    case 8: { // key_value_metadata: list<struct>; skip
      auto lh = ReadListHeader(in);
      for (uint32_t i = 0; i < lh.size; i++) {
        int16_t kv_last = 0;
        std::string key, value;
        while (true) {
          auto kvfh = ReadFieldHeader(in, kv_last);
          if (kvfh.type == 0)
            break;
          switch (kvfh.id) {
          case 1:
            key = ReadString(in);
            break;
          case 2:
            value = ReadString(in);
            break;
          default:
            SkipField(in, kvfh.type);
            break;
          }
        }
        if (!key.empty()) {
          cs.key_value_metadata.emplace(std::move(key), std::move(value));
        }
      }
      break;
    }
    case 9: {
      cs.data_page_offset = ReadI64(in);
      break;
    }
    case 10: {
      cs.index_page_offset = ReadI64(in);
      break;
    }
    case 11: {
      cs.dictionary_page_offset = ReadI64(in);
      break;
    }
    case 12: {
      if (opts.include_statistics) {
        ParseStatistics(in, cs);
      } else {
        SkipField(in, fh.type);
      }
      break;
    } // statistics
    case 14: {
      if (opts.include_statistics) {
        cs.bloom_offset = ReadI64(in);
      } else {
        (void)ReadI64(in);
      }
      break;
    } // bloom_filter_offset (common)
    case 15: {
      if (opts.include_statistics) {
        cs.bloom_length = ReadI64(in);
      } else {
        (void)ReadI64(in);
      }
      break;
    } // bloom_filter_length (common)
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// parse a ColumnChunk, and descend into meta_data when present
static void ParseColumnChunk(TInput &in, ColumnStats &out,
                             const MetadataParseOptions &opts) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    switch (fh.id) {
    case 1: {
      (void)ReadString(in);
      break;
    } // file_path
    case 2: {
      out.column_chunk_file_offset = ReadI64(in);
      break;
    } // file_offset
    case 3: { // meta_data (ColumnMetaData)
      ParseColumnMeta(in, out, opts);
      break;
    }
    // skip everything else
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// FIX: correct RowGroup field IDs (columns=1, total_byte_size=2, num_rows=3)
static void ParseRowGroup(TInput &in, RowGroupStats &rg,
                          const MetadataParseOptions &opts) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 1: { // columns: list<ColumnChunk>
      auto lh = ReadListHeader(in);
      rg.columns.reserve(lh.size);
      for (uint32_t i = 0; i < lh.size; i++) {
        ColumnStats cs;
        ParseColumnChunk(in, cs, opts); // <-- go via ColumnChunk
        rg.columns.push_back(std::move(cs));
      }
      break;
    }
    case 2:
      rg.total_byte_size = ReadI64(in);
      break;
    case 3:
      rg.num_rows = ReadI64(in);
      break;
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

// ------------------- Schema Walker -------------------

static std::vector<SchemaElement>
WalkSchema(TInput &in, int remaining, const std::string &parent_path = "") {
  std::vector<SchemaElement> nodes;
  nodes.reserve(remaining);

  for (int i = 0; i < remaining; i++) {
    SchemaElement elem = ParseSchemaElement(in);
    elem.full_name =
        parent_path.empty() ? elem.name : parent_path + "." + elem.name;

    if (elem.num_children > 0) {
      elem.children = WalkSchema(in, elem.num_children, elem.full_name);
    }

    nodes.push_back(std::move(elem));
  }
  return nodes;
}

static inline bool IsOptional(const SchemaElement &elem) {
  return elem.repetition_type == 1;
}

static std::string ResolveArrayLogicalType(const SchemaElement &elem) {
  std::string child_type = "unknown";
  if (!elem.children.empty()) {
    const SchemaElement *cur = &elem.children[0];
    while (cur) {
      if (!cur->logical_type.empty() && cur->logical_type != "struct" &&
          cur->logical_type != "array") {
        child_type = cur->logical_type;
        break;
      }
      if (!cur->physical_type.empty() && cur->logical_type.empty() &&
          cur->children.empty()) {
        child_type = cur->physical_type;
        break;
      }
      if (cur->children.empty())
        break;
      cur = &cur->children[0];
    }
  }
  return "array<" + child_type + ">";
}

static void EmitSchemaEntry(const SchemaElement &elem, bool ancestor_optional,
                            bool is_top_level,
                            std::vector<SchemaField> &columns,
                            std::unordered_map<std::string, std::string> &map) {
  const bool nullable = ancestor_optional || IsOptional(elem);
  const std::string canonical = CanonicalizeColumnName(
      elem.full_name.empty() ? elem.name : elem.full_name);

  if (elem.logical_type == "struct") {
    if (is_top_level) {
      SchemaField field;
      field.name = canonical;
      field.physical_type = "struct";
      field.logical_type = "json";
      field.nullable = nullable;
      columns.push_back(std::move(field));
    }

    map[canonical] = "json";
    if (elem.name != canonical) {
      map[elem.name] = "json";
    }

    for (const auto &child : elem.children) {
      EmitSchemaEntry(child, nullable, false, columns, map);
    }
    return;
  }

  if (elem.logical_type == "array") {
    const std::string array_type = ResolveArrayLogicalType(elem);
    if (is_top_level) {
      SchemaField field;
      field.name = canonical;
      field.physical_type = "list";
      field.logical_type = array_type;
      field.nullable = nullable;
      columns.push_back(std::move(field));
    }

    map[canonical] = array_type;
    if (elem.name != canonical) {
      map[elem.name] = array_type;
    }
    return;
  }

  std::string logical = elem.logical_type;
  if (logical.empty()) {
    if (elem.type_length > 0 && elem.physical_type == "fixed_len_byte_array") {
      logical =
          "fixed_len_byte_array[" + std::to_string(elem.type_length) + "]";
    } else if (elem.physical_type == "byte_array") {
      logical = "binary";
    } else if (elem.physical_type == "fixed_len_byte_array") {
      logical = "binary";
    } else if (!elem.physical_type.empty()) {
      logical = elem.physical_type;
    } else {
      logical = "unknown";
    }
  }

  if (is_top_level) {
    SchemaField field;
    field.name = canonical;
    field.physical_type =
        elem.physical_type.empty() ? logical : elem.physical_type;
    field.logical_type = logical;
    field.nullable = nullable;
    columns.push_back(std::move(field));
  }

  map[canonical] = logical;
  if (elem.name != canonical) {
    map[elem.name] = logical;
  }
}

static void
CollectSchemaArtifacts(const SchemaElement &root,
                       std::vector<SchemaField> &columns,
                       std::unordered_map<std::string, std::string> &map) {
  for (const auto &child : root.children) {
    EmitSchemaEntry(child, false, true, columns, map);
  }
}

static FileStats ParseFileMeta(TInput &in, const MetadataParseOptions &opts) {
  FileStats fs;

  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;

    switch (fh.id) {
    case 2: { // schema (list<SchemaElement>)
      ReadListHeader(in);
      fs.schema = WalkSchema(in, 1);
      break;
    }
    case 3:
      fs.num_rows = ReadI64(in);
      break;
    case 4: { // row_groups (list<RowGroup>)
      auto lh = ReadListHeader(in);
      if (opts.schema_only) {
        for (uint32_t i = 0; i < lh.size; i++) {
          SkipStruct(in);
        }
      } else {
        uint32_t limit = lh.size;
        if (opts.max_row_groups >= 0) {
          limit = std::min<uint32_t>(
              lh.size, static_cast<uint32_t>(opts.max_row_groups));
        }
        fs.row_groups.reserve(limit);
        for (uint32_t i = 0; i < lh.size; i++) {
          if (i < limit) {
            RowGroupStats rg;
            ParseRowGroup(in, rg, opts);
            fs.row_groups.push_back(std::move(rg));
          } else {
            SkipStruct(in);
          }
        }
      }
      break;
    }
    default:
      SkipField(in, fh.type);
      break;
    }
  }
  return fs;
}
static void ApplyLogicalTypes(
    FileStats &fs,
    const std::unordered_map<std::string, std::string> &logical_type_map) {
  if (fs.row_groups.empty()) {
    return;
  }

  for (auto &rg : fs.row_groups) {
    for (auto &col : rg.columns) {
      auto it = logical_type_map.find(col.name);
      if (it != logical_type_map.end()) {
        col.logical_type = it->second;
        continue;
      }

      if (col.logical_type.empty()) {
        if (col.physical_type == "int96") {
          col.logical_type = "timestamp[ns]";
        } else if (col.physical_type == "byte_array") {
          col.logical_type = "binary";
        } else if (col.physical_type == "fixed_len_byte_array") {
          col.logical_type = "binary";
        } else if (!col.physical_type.empty()) {
          col.logical_type = col.physical_type;
        } else {
          col.logical_type = "unknown";
        }
      }
    }
  }
}

// Enrich column stats with schema information for level data
static void EnrichColumnStatsWithSchemaInfo(FileStats &fs) {
  if (fs.schema.empty()) {
    return;
  }

  // Build a map from column name to schema element
  std::unordered_map<std::string, const SchemaElement*> schema_map;
  
  std::function<void(const SchemaElement&, int)> walk_schema = 
    [&](const SchemaElement& elem, int depth) {
      std::string canonical = CanonicalizeColumnName(
        elem.full_name.empty() ? elem.name : elem.full_name);
      schema_map[canonical] = &elem;
      
      for (const auto& child : elem.children) {
        walk_schema(child, depth + 1);
      }
    };
  
  // Walk the schema starting from root
  for (const auto& root : fs.schema) {
    for (const auto& child : root.children) {
      walk_schema(child, 1);
    }
  }

  // Enrich each column in each row group
  for (auto& rg : fs.row_groups) {
    for (auto& col : rg.columns) {
      auto it = schema_map.find(col.name);
      if (it != schema_map.end()) {
        const SchemaElement* schema_elem = it->second;
        col.repetition_type = schema_elem->repetition_type;
        
        // For non-nested columns (path contains no dots), max_repetition_level = 0
        col.max_repetition_level = 0;  // We only support flat schemas
        
        // max_definition_level depends on whether column is required or optional
        // 0 = REQUIRED, 1 = OPTIONAL, 2 = REPEATED
        if (schema_elem->repetition_type == 0) {
          // REQUIRED: no nulls possible
          col.max_definition_level = 0;
        } else {
          // OPTIONAL or REPEATED: nulls possible
          col.max_definition_level = 1;
        }
      }
    }
  }
}

// ------------------- Entry point -------------------

FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size,
                                        const MetadataParseOptions &opts) {
  if (size < 8) {
    throw std::runtime_error("Buffer too small");
  }

  // trailer is always last 8 bytes
  const uint8_t *trailer = buf + size - 8;

  if (memcmp(trailer + 4, "PAR1", 4) != 0)
    throw std::runtime_error("Not a parquet file");

  uint32_t footer_len = ReadLE32(trailer);
  if (footer_len + 8 > size)
    throw std::runtime_error("Footer length invalid");

  const uint8_t *footer_start = buf + size - 8 - footer_len;
  const uint8_t *footer_end = buf + size - 8;

  TInput in{footer_start, footer_end};
  FileStats fs = ParseFileMeta(in, opts);

  std::unordered_map<std::string, std::string> logical_type_map;
  if (!fs.schema.empty()) {
    for (const auto &root : fs.schema) {
      if (root.children.empty()) {
        continue;
      }
      CollectSchemaArtifacts(root, fs.schema_columns, logical_type_map);
      break;
    }
  }

  // Apply map to row group columns
  ApplyLogicalTypes(fs, logical_type_map);

  // Enrich column stats with schema information (repetition type, max levels)
  EnrichColumnStatsWithSchemaInfo(fs);

  return fs;
}

FileStats ReadParquetMetadataFromBuffer(const uint8_t *buf, size_t size) {
  MetadataParseOptions opts;
  return ReadParquetMetadataFromBuffer(buf, size, opts);
}

FileStats ReadParquetMetadata(const std::string &path,
                              const MetadataParseOptions &options) {
  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open parquet file: " + path);
  }

  file.seekg(0, std::ios::end);
  const std::streamoff file_size = file.tellg();
  if (file_size < 8) {
    throw std::runtime_error("File too small to be a parquet file");
  }

  file.seekg(file_size - 8);
  uint8_t trailer[8];
  file.read(reinterpret_cast<char *>(trailer), 8);
  if (file.gcount() != 8) {
    throw std::runtime_error("Failed to read parquet footer");
  }

  if (std::memcmp(trailer + 4, "PAR1", 4) != 0) {
    throw std::runtime_error("Not a parquet file");
  }

  const uint32_t footer_len = ReadLE32(trailer);
  if (static_cast<uint64_t>(footer_len) + 8 >
      static_cast<uint64_t>(file_size)) {
    throw std::runtime_error("Footer length invalid");
  }

  std::vector<uint8_t> buffer(static_cast<size_t>(footer_len) + 8);
  file.seekg(file_size - 8 - footer_len);
  file.read(reinterpret_cast<char *>(buffer.data()), footer_len);
  if (file.gcount() != static_cast<std::streamsize>(footer_len)) {
    throw std::runtime_error("Failed to read parquet footer metadata");
  }

  std::memcpy(buffer.data() + footer_len, trailer, 8);

  return ReadParquetMetadataFromBuffer(buffer.data(), buffer.size(), options);
}

FileStats ReadParquetMetadata(const std::string &path) {
  MetadataParseOptions opts;
  return ReadParquetMetadata(path, opts);
}
