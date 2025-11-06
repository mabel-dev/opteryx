#include "metadata.hpp"
#include "thrift.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct BloomFilterHeaderData {
  int32_t filter_type = 0;
  int32_t hash = 0;
  int32_t num_bytes = 0;
  int32_t num_hashes = 0;
  int32_t bitset_size = 0;
  size_t header_bytes = 0;
};

enum BloomFilterType {
  BLOOM_FILTER_UNKNOWN = 0,
  BLOOM_FILTER_SPLIT_BLOCK = 1,
};

enum BloomFilterHash {
  BLOOM_HASH_UNKNOWN = 0,
  BLOOM_HASH_XXHASH = 1,
};

constexpr uint32_t kBytesPerBlock = 32;
constexpr uint32_t kWordsPerBlock = 8;

constexpr uint32_t kBloomFilterSalts[kWordsPerBlock] = {
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31};

inline uint64_t ReadLittleEndian64(const uint8_t* ptr) {
  uint64_t value;
  std::memcpy(&value, ptr, sizeof(uint64_t));
  return value;
}

inline uint32_t ReadLittleEndian32(const uint8_t* ptr) {
  uint32_t value;
  std::memcpy(&value, ptr, sizeof(uint32_t));
  return value;
}

void ParseBloomFilterHeader(TInput& in, BloomFilterHeaderData& header) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) {
      break;
    }

    switch (fh.id) {
    case 1:
      header.filter_type = ReadI32(in);
      break;
    case 2:
      header.hash = ReadI32(in);
      break;
    case 3:
      header.num_bytes = ReadI32(in);
      break;
    case 4:
      header.num_hashes = ReadI32(in);
      break;
    case 5:
      header.bitset_size = ReadI32(in);
      break;
    default:
      SkipField(in, fh.type);
      break;
    }
  }
}

void ParseBloomFilterPayload(TInput& in, BloomFilterHeaderData& header,
                             std::vector<uint8_t>& bitset) {
  const uint8_t* start = in.p;
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) {
      break;
    }

    switch (fh.id) {
    case 1: {
      if (fh.type == ThriftType::T_STRUCT) {
        ParseBloomFilterHeader(in, header);
      } else if (fh.type == 5 || fh.type == 4) {
        header.num_bytes = ReadI32(in);
      } else if (fh.type == ThriftType::T_STRING) {
        std::string payload = ReadString(in);
        bitset.assign(payload.begin(), payload.end());
      } else {
        SkipField(in, fh.type);
      }
      break;
    }
    case 2: {
      if (fh.type == ThriftType::T_STRUCT) {
        header.filter_type = BLOOM_FILTER_SPLIT_BLOCK;
        SkipStruct(in);
      } else {
        SkipField(in, fh.type);
      }
      break;
    }
    case 3: {
      if (fh.type == ThriftType::T_STRUCT) {
        header.hash = BLOOM_HASH_XXHASH;
        SkipStruct(in);
      } else if (fh.type == 5 || fh.type == 4) {
        header.hash = ReadI32(in);
      } else {
        SkipField(in, fh.type);
      }
      break;
    }
    case 4: {
      if (fh.type == ThriftType::T_STRUCT) {
        SkipStruct(in);
      } else {
        SkipField(in, fh.type);
      }
      break;
    }
    default:
      SkipField(in, fh.type);
      break;
    }
  }

  header.header_bytes = static_cast<size_t>(in.p - start);
}

inline uint64_t Rotl64(uint64_t x, uint32_t r) {
  return (x << r) | (x >> (64 - r));
}

uint64_t XxHash64(const uint8_t* data, size_t len, uint64_t seed = 0) {
  constexpr uint64_t PRIME64_1 = 11400714785074694791ULL;
  constexpr uint64_t PRIME64_2 = 14029467366897019727ULL;
  constexpr uint64_t PRIME64_3 = 1609587929392839161ULL;
  constexpr uint64_t PRIME64_4 = 9650029242287828579ULL;
  constexpr uint64_t PRIME64_5 = 2870177450012600261ULL;

  uint64_t hash;
  const uint8_t* ptr = data;
  const uint8_t* end = data + len;

  if (len >= 32) {
    uint64_t v1 = seed + PRIME64_1 + PRIME64_2;
    uint64_t v2 = seed + PRIME64_2;
    uint64_t v3 = seed + 0;
    uint64_t v4 = seed - PRIME64_1;

    const uint8_t* limit = end - 32;
    do {
      v1 = Rotl64(v1 + ReadLittleEndian64(ptr) * PRIME64_2, 31) * PRIME64_1;
      ptr += 8;
      v2 = Rotl64(v2 + ReadLittleEndian64(ptr) * PRIME64_2, 31) * PRIME64_1;
      ptr += 8;
      v3 = Rotl64(v3 + ReadLittleEndian64(ptr) * PRIME64_2, 31) * PRIME64_1;
      ptr += 8;
      v4 = Rotl64(v4 + ReadLittleEndian64(ptr) * PRIME64_2, 31) * PRIME64_1;
      ptr += 8;
    } while (ptr <= limit);

    hash = Rotl64(v1, 1) + Rotl64(v2, 7) + Rotl64(v3, 12) + Rotl64(v4, 18);

    v1 = Rotl64(v1 * PRIME64_2, 31) * PRIME64_1;
    hash ^= v1;
    hash = hash * PRIME64_1 + PRIME64_4;

    v2 = Rotl64(v2 * PRIME64_2, 31) * PRIME64_1;
    hash ^= v2;
    hash = hash * PRIME64_1 + PRIME64_4;

    v3 = Rotl64(v3 * PRIME64_2, 31) * PRIME64_1;
    hash ^= v3;
    hash = hash * PRIME64_1 + PRIME64_4;

    v4 = Rotl64(v4 * PRIME64_2, 31) * PRIME64_1;
    hash ^= v4;
    hash = hash * PRIME64_1 + PRIME64_4;
  } else {
    hash = seed + PRIME64_5;
  }

  hash += static_cast<uint64_t>(len);

  while (ptr + 8 <= end) {
    uint64_t k1 = Rotl64(ReadLittleEndian64(ptr) * PRIME64_2, 31) * PRIME64_1;
    hash ^= k1;
    hash = Rotl64(hash, 27) * PRIME64_1 + PRIME64_4;
    ptr += 8;
  }

  if (ptr + 4 <= end) {
    hash ^= static_cast<uint64_t>(ReadLittleEndian32(ptr)) * PRIME64_1;
    hash = Rotl64(hash, 23) * PRIME64_2 + PRIME64_3;
    ptr += 4;
  }

  while (ptr < end) {
    hash ^= static_cast<uint64_t>(*ptr) * PRIME64_5;
    hash = Rotl64(hash, 11) * PRIME64_1;
    ++ptr;
  }

  hash ^= hash >> 33;
  hash *= PRIME64_2;
  hash ^= hash >> 29;
  hash *= PRIME64_3;
  hash ^= hash >> 32;

  return hash;
}

void ComputeMask(uint32_t hash, uint32_t mask[kWordsPerBlock]) {
  for (uint32_t i = 0; i < kWordsPerBlock; ++i) {
    uint32_t product = hash * kBloomFilterSalts[i];
    uint32_t bit = (product >> 27) & 31U;
    mask[i] = 1U << bit;
  }
}

bool SplitBlockContains(const std::vector<uint8_t>& bitset,
                        uint32_t num_bytes_from_header,
                        const std::string& value) {
  if (bitset.empty()) {
    return false;
  }
  if (bitset.size() % kBytesPerBlock != 0) {
    throw std::runtime_error("Bloom filter bitset not aligned to block size");
  }
  if (num_bytes_from_header > 0 &&
      static_cast<size_t>(num_bytes_from_header) != bitset.size()) {
    throw std::runtime_error("Bloom filter header size mismatch");
  }

  uint32_t num_blocks = static_cast<uint32_t>(bitset.size() / kBytesPerBlock);
  if (num_blocks == 0) {
    return false;
  }

  uint64_t hash64 =
      XxHash64(reinterpret_cast<const uint8_t*>(value.data()), value.size());
  uint32_t low_hash = static_cast<uint32_t>(hash64 & 0xFFFFFFFFULL);
  uint32_t block_index = 0;
  if (num_blocks > 1) {
    uint32_t log_num_blocks = 0;
    while ((1U << log_num_blocks) < num_blocks) {
      ++log_num_blocks;
    }
    if ((1U << log_num_blocks) == num_blocks) {
      uint64_t selector = hash64 >> (64 - log_num_blocks);
      block_index = static_cast<uint32_t>(selector) & (num_blocks - 1);
    } else {
      uint32_t high_hash = static_cast<uint32_t>((hash64 >> 32) & 0xFFFFFFFFULL);
      block_index = high_hash % num_blocks;
    }
  }
  const uint8_t* block_ptr = bitset.data() + static_cast<size_t>(block_index) * kBytesPerBlock;

  uint32_t mask[kWordsPerBlock];
  ComputeMask(low_hash, mask);

  uint32_t words[kWordsPerBlock];
  std::memcpy(words, block_ptr, kBytesPerBlock);

  for (uint32_t i = 0; i < kWordsPerBlock; ++i) {
    if ((words[i] & mask[i]) != mask[i]) {
      return false;
    }
  }
  return true;
}

} // namespace

bool TestBloomFilter(const std::string& file_path, int64_t bloom_offset,
                     int64_t bloom_length, const std::string& value) {
  if (bloom_offset < 0) {
    throw std::invalid_argument("Bloom filter offset must be non-negative");
  }

  std::ifstream input(file_path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("Unable to open parquet file for bloom filter");
  }

  if (!input.seekg(bloom_offset)) {
    throw std::runtime_error("Failed to seek to bloom filter offset");
  }

  std::vector<uint8_t> buffer;
  if (bloom_length > 0) {
    buffer.resize(static_cast<size_t>(bloom_length));
    input.read(reinterpret_cast<char*>(buffer.data()),
               static_cast<std::streamsize>(bloom_length));
    if (input.gcount() != bloom_length) {
      throw std::runtime_error("Failed to read bloom filter data");
    }
  } else {
    std::streampos current = input.tellg();
    if (!input.seekg(0, std::ios::end)) {
      throw std::runtime_error("Failed to determine bloom filter length");
    }
    std::streampos end_pos = input.tellg();
    if (end_pos < current) {
      throw std::runtime_error("Invalid bloom filter offsets");
    }
    size_t remaining = static_cast<size_t>(end_pos - current);
    if (remaining == 0) {
      return false;
    }
    buffer.resize(remaining);
    if (!input.seekg(current)) {
      throw std::runtime_error("Failed to seek back to bloom filter data");
    }
    input.read(reinterpret_cast<char*>(buffer.data()),
               static_cast<std::streamsize>(remaining));
    if (static_cast<size_t>(input.gcount()) != remaining) {
      throw std::runtime_error("Failed to read bloom filter data");
    }
  }

  TInput in;
  in.p = buffer.data();
  in.end = buffer.data() + buffer.size();

  BloomFilterHeaderData header;
  std::vector<uint8_t> bitset;
  ParseBloomFilterPayload(in, header, bitset);

  if (header.filter_type != BLOOM_FILTER_SPLIT_BLOCK && header.filter_type != BLOOM_FILTER_UNKNOWN) {
    throw std::runtime_error("Unsupported bloom filter type");
  }
  if (header.hash != BLOOM_HASH_XXHASH && header.hash != BLOOM_HASH_UNKNOWN) {
    throw std::runtime_error("Unsupported bloom filter hash");
  }

  if (bitset.empty()) {
    size_t available = static_cast<size_t>(in.end - in.p);
    size_t expected = 0;
    if (header.num_bytes > 0) {
      expected = static_cast<size_t>(header.num_bytes);
    } else if (bloom_length > 0 &&
               static_cast<size_t>(bloom_length) > header.header_bytes) {
      expected = static_cast<size_t>(bloom_length) - header.header_bytes;
    } else {
      expected = available;
    }

    if (expected == 0) {
      return false;
    }
    if (available < expected) {
      throw std::runtime_error("Bloom filter bitset truncated");
    }
    bitset.assign(in.p, in.p + expected);
    if (header.num_bytes <= 0) {
      header.num_bytes = static_cast<int32_t>(expected);
    }
  }

  if (bitset.empty()) {
    return false;
  }

  return SplitBlockContains(bitset, header.num_bytes, value);
}
