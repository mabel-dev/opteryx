#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void simd_mix_hash(uint64_t* dest, const uint64_t* values, size_t count, uint64_t mix_constant);

#ifdef __cplusplus
}
#endif
