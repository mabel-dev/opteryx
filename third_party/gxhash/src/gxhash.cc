#include "gxhash.h"

extern "C" uint32_t gx_hash_32(const void* data, size_t length) {
    return gxhash::gxhash32(reinterpret_cast<const uint8_t*>(data), length, 0);
}