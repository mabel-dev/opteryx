#pragma once
#include <cstdint>
#include <vector>
#include <string>

namespace rugo {
namespace compression {

enum class CompressionCodec {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6
};

// Main decompression function
// Returns decompressed data as a vector
std::vector<uint8_t> DecompressData(
    const uint8_t* compressed_data,
    size_t compressed_size,
    size_t uncompressed_size,
    CompressionCodec codec
);

// Codec-specific implementations
std::vector<uint8_t> DecompressSnappy(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size
);

std::vector<uint8_t> DecompressZstd(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size
);

// Future extension point for GZIP
std::vector<uint8_t> DecompressGzip(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size
);

// Helper to convert parquet codec integers to our enum
CompressionCodec CodecFromInt(int32_t codec_int);

// Helper to get codec name for debugging
std::string CodecName(CompressionCodec codec);

}  // namespace compression
}  // namespace rugo