#include "simd_env.h"
#include "cpu_features.h"

#include <cstdlib>
#include <cstdio>

static bool env_bool(const char* name) {
    const char* v = std::getenv(name);
    return v != nullptr && v[0] != '\0';
}

void opteryx_check_simd_env_or_abort() {
    // Fail-if flags first
    if (env_bool("OPTERYX_FAIL_IF_NOT_AVX2")) {
        if (!cpu_supports_avx2()) {
            std::fprintf(stderr, "OPTERYX_FAIL_IF_NOT_AVX2 set but CPU lacks AVX2\n");
            std::abort();
        }
    }
}

int opteryx_cpu_supports_avx2() { return cpu_supports_avx2() ? 1 : 0; }
int opteryx_cpu_supports_neon() { return cpu_supports_neon() ? 1 : 0; }
