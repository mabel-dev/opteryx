#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Shared mixing constant used by scalar and SIMD mixers.
#ifndef MIX_HASH_CONSTANT
#define MIX_HASH_CONSTANT ((uint64_t)0x9e3779b97f4a7c15ULL)
#endif

void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count);

#ifdef __cplusplus
}
#endif
