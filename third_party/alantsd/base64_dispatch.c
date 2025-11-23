#include "base64.h"
#include <string.h>

static int cpu_features_detected = 0;
static b64_cpu_features features = {0};

#ifdef __x86_64__
#include <cpuid.h>

static void x86_cpuid(int function, int subfunction, int* cpuinfo) {
    __cpuid_count(function, subfunction, cpuinfo[0], cpuinfo[1], cpuinfo[2], cpuinfo[3]);
}

static int check_x86_feature(int feature) {
    int cpuinfo[4];
    x86_cpuid(1, 0, cpuinfo);
    return (cpuinfo[2] & feature) != 0;
}

static int check_avx2(void) {
    int cpuinfo[4];
    x86_cpuid(7, 0, cpuinfo);
    return (cpuinfo[1] & (1 << 5)) != 0;
}

static int check_avx512(void) {
    int cpuinfo[4];
    x86_cpuid(7, 0, cpuinfo);
    // Check for AVX512F (bit 16) and AVX512BW (bit 30) in EBX (cpuinfo[1])
    // AVX512F: Foundation instructions (required for all AVX512)
    // AVX512BW: Byte and Word instructions (required for our string operations)
    int has_avx512f = (cpuinfo[1] & (1 << 16)) != 0;
    int has_avx512bw = (cpuinfo[1] & (1 << 30)) != 0;
    return has_avx512f && has_avx512bw;
}
#endif

b64_cpu_features b64_detect_cpu_features(void) {
    if (cpu_features_detected) {
        return features;
    }

    memset(&features, 0, sizeof(features));

    // NEON detection (ARM)
#if defined(__ARM_NEON) || defined(__aarch64__)
    features.neon = 1;
#endif

    // AVX2 and AVX512 detection (x86)
#ifdef __x86_64__
    if (check_x86_feature(1 << 27)) { // OSXSAVE
        if (check_x86_feature(1 << 28)) { // AVX
            features.avx2 = check_avx2();
            features.avx512 = check_avx512();
        }
    }
#endif

    cpu_features_detected = 1;
    return features;
}

void b64_force_scalar(void) {
    features.neon = 0;
    features.avx2 = 0;
    features.avx512 = 0;
    cpu_features_detected = 1;
}

int b64_has_neon(void) {
    if (!cpu_features_detected) {
        b64_detect_cpu_features();
    }
    return features.neon;
}

int b64_has_avx2(void) {
    if (!cpu_features_detected) {
        b64_detect_cpu_features();
    }
    return features.avx2;
}

int b64_has_avx512(void) {
    if (!cpu_features_detected) {
        b64_detect_cpu_features();
    }
    return features.avx512;
}

// Auto-dispatch implementations for core API
void* b64tobin_len(void* restrict dest, const char* restrict src, size_t len) {
    if (!cpu_features_detected) {
        b64_detect_cpu_features();
    }

    if (features.avx512 && len >= 64) {
        return b64tobin_avx512(dest, src, len);
    } else if (features.avx2 && len >= 32) {
        return b64tobin_avx2(dest, src, len);
    } else if (features.neon && len >= 16) {
        return b64tobin_neon(dest, src, len);
    } else {
        return b64tobin_scalar(dest, src, len);
    }
}

void* b64tobin(void* restrict dest, const char* restrict src) {
    return b64tobin_len(dest, src, strlen(src));
}

char* bintob64(char* restrict dest, const void* restrict src, size_t size) {
    if (!cpu_features_detected) {
        b64_detect_cpu_features();
    }

    if (features.avx512 && size >= 48) {
        return bintob64_avx512(dest, src, size);
    } else if (features.avx2 && size >= 24) {
        return bintob64_avx2(dest, src, size);
    } else if (features.neon && size >= 12) {
        return bintob64_neon(dest, src, size);
    } else {
        return bintob64_scalar(dest, src, size);
    }
}