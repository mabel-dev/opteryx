#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

struct TInput {
  const uint8_t *p;
  const uint8_t *end;

  uint8_t readByte() {
    if (p >= end)
      throw std::runtime_error("EOF");
    return *p++;
  }
};

enum ThriftType {
  T_STOP = 0,
  T_BOOL_TRUE = 1,
  T_BOOL_FALSE = 2,
  T_BOOL = 3,
  T_BYTE = 4,
  T_I16 = 6,
  T_I32 = 8,
  T_I64 = 10,
  T_DOUBLE = 11,
  T_STRING = 12,
  T_STRUCT = 13,
  T_MAP = 14,
  T_SET = 15,
  T_LIST = 16,
  // … whatever else you have
};

// ------------------- Varint / ZigZag -------------------

inline uint64_t ReadVarint(TInput &in) {
  uint64_t result = 0;
  int shift = 0;
  int count = 0;
  while (true) {
    if (count++ > 10)
      throw std::runtime_error("Varint too long");
    uint8_t byte = in.readByte();
    result |= (uint64_t)(byte & 0x7F) << shift;
    if (!(byte & 0x80))
      break;
    if (shift >= 63)
      throw std::runtime_error("Varint overflow");
    shift += 7;
  }
  return result;
}

inline int64_t ZigZagDecode(uint64_t n) { return (n >> 1) ^ -(int64_t)(n & 1); }

inline int64_t ReadI64(TInput &in) { return ZigZagDecode(ReadVarint(in)); }

inline int32_t ReadI32(TInput &in) {
  return (int32_t)ZigZagDecode(ReadVarint(in));
}

inline std::string ReadString(TInput &in) {
  uint64_t len = ReadVarint(in);
  uint64_t avail = (uint64_t)(in.end - in.p);
  if (len > avail)
    throw std::runtime_error("Invalid string length");
  std::string s((const char *)in.p, (size_t)len);
  in.p += len;
  return s;
}

static inline bool ReadBool(TInput &in) { return in.readByte() != 0; }

// ------------------- Compact Protocol Structs -------------------

struct FieldHeader {
  int16_t id;
  uint8_t type;
};

// Decode a field header (Thrift Compact Protocol)
inline FieldHeader ReadFieldHeader(TInput &in, int16_t &last_id) {
  uint8_t header = in.readByte();
  if (header == 0) {
    // STOP marker: do not touch last_id, just return
    return {0, 0};
  }

  uint8_t type = header & 0x0F;
  uint8_t modifier = header >> 4;

  int16_t field_id;
  if (modifier == 0) {
    field_id = static_cast<int16_t>(ZigZagDecode(ReadVarint(in)));
  } else {
    // Delta from previous id
    field_id = static_cast<int16_t>(last_id + modifier);
  }

  last_id = field_id; // update only when not STOP
  return {field_id, type};
}

// Compact list header
struct ListHeader {
  uint8_t elem_type;
  uint32_t size;
};

inline ListHeader ReadListHeader(TInput &in) {
  uint8_t first = in.readByte();
  uint32_t size = first >> 4;
  uint8_t elem_type = first & 0x0F;
  if (size == 15) {
    size = (uint32_t)ReadVarint(in);
  }
  return {elem_type, size};
}

inline void SkipField(TInput &in, uint8_t type) {
  switch (type) {
  case 0:
    return; // STOP
  case 1:
  case 2:
    return; // BOOL
  case 3:
    in.readByte();
    return; // BYTE
  case 4:
    (void)ReadI32(in);
    return; // I16 zigzag
  case 5:
    (void)ReadI32(in);
    return; // I32 zigzag
  case 6:
    (void)ReadI64(in);
    return; // I64 zigzag
  case 7: { // DOUBLE
    if ((size_t)(in.end - in.p) < 8)
      throw std::runtime_error("EOF");
    in.p += 8;
    return;
  }
  case 8:
    (void)ReadString(in);
    return; // BINARY/STRING
  case 9: { // LIST
    auto lh = ReadListHeader(in);
    for (uint32_t i = 0; i < lh.size; i++) {
      if (lh.elem_type == 1 || lh.elem_type == 2) {
        in.readByte(); // consume boolean element byte
      } else {
        SkipField(in, lh.elem_type);
      }
    }
    return;
  }
  case 10: { // SET
    auto lh = ReadListHeader(in);
    for (uint32_t i = 0; i < lh.size; i++) {
      if (lh.elem_type == 1 || lh.elem_type == 2) {
        in.readByte();
      } else {
        SkipField(in, lh.elem_type);
      }
    }
    return;
  }
  case 11: { // MAP
    uint8_t first = in.readByte();
    uint32_t size = first >> 4;
    if (size == 0)
      return;
    if (size == 15)
      size = (uint32_t)ReadVarint(in);
    uint8_t types = in.readByte();
    uint8_t key_type = types >> 4;
    uint8_t val_type = types & 0x0F;

    for (uint32_t i = 0; i < size; i++) {
      if (key_type == 1 || key_type == 2)
        in.readByte();
      else
        SkipField(in, key_type);
      if (val_type == 1 || val_type == 2)
        in.readByte();
      else
        SkipField(in, val_type);
    }
    return;
  }
  case 12: { // STRUCT
    int16_t last = 0;
    while (true) {
      auto fh = ReadFieldHeader(in, last);
      if (fh.type == 0)
        break;
      SkipField(in, fh.type);
    }
    return;
  }
  default:
    // Be forgiving: consume one byte to move on
    in.readByte();
    return;
  }
}

static void SkipStruct(TInput &in) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0)
      break;
    SkipField(in, fh.type);
  }
}
