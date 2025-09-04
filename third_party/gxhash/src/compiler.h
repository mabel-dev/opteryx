#pragma once

#if defined(_MSC_VER)
#define GXHASH_ALWAYS_INLINE static inline __forceinline
#else
#define GXHASH_ALWAYS_INLINE static inline __attribute__((always_inline))
#endif
