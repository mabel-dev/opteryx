#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// Check environment-driven policies and abort if configured and requirements
// are not met.
void opteryx_check_simd_env_or_abort();

// Simple wrappers to expose CPU probe results to external modules.
int opteryx_cpu_supports_avx2();
int opteryx_cpu_supports_neon();

#ifdef __cplusplus
}
#endif
