#ifndef BASE64_H
#define BASE64_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Lookup tables (shared across implementations)
extern const uint8_t B64_DECODE_LUT[256];
extern const char B64_ENCODE_LUT[64];

// Basic functions (with auto-dispatch)
void* b64tobin(void* restrict dest, const char* restrict src);
void* b64tobin_len(void* restrict dest, const char* restrict src, size_t len);
char* bintob64(char* dest, const void* src, size_t size);

// Optimized versions (for direct use if needed)
void* b64tobin_scalar(void* restrict dest, const char* restrict src, size_t len);
void* b64tobin_neon(void* restrict dest, const char* restrict src, size_t len);
void* b64tobin_avx2(void* restrict dest, const char* restrict src, size_t len);
void* b64tobin_avx512(void* restrict dest, const char* restrict src, size_t len);

char* bintob64_scalar(char* restrict dest, const void* restrict src, size_t size);
char* bintob64_neon(char* restrict dest, const void* restrict src, size_t size);
char* bintob64_avx2(char* restrict dest, const void* restrict src, size_t size);
char* bintob64_avx512(char* restrict dest, const void* restrict src, size_t size);

// Utility functions
size_t b64_encoded_size(size_t bin_size);
size_t b64_decoded_size(size_t b64_len);

// CPU feature detection
typedef struct {
    int neon;
    int avx2;
    int avx512;
} b64_cpu_features;

b64_cpu_features b64_detect_cpu_features(void);
void b64_force_scalar(void);  // Force scalar implementation
int b64_has_neon(void);  // Check if NEON is available
int b64_has_avx2(void);  // Check if AVX2 is available
int b64_has_avx512(void);  // Check if AVX512 is available

#ifdef __cplusplus
}
#endif

#endif /* BASE64_H */