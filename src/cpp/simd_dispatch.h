#pragma once

#include <atomic>
#include <cstdlib>
#include <mutex>
#include <initializer_list>
#include <utility>
#include <cstddef>
#include "cpu_features.h"

// Helper to select a runtime dispatch function pointer based on CPU feature checks.
// Usage:
//   using fn_t = R(*)(Args...);
//   static std::atomic<fn_t> cache{nullptr};
//   fn_t fn = simd::select_dispatch<fn_t>(cache, {
//   #if defined(__AVX2__)
//       { &cpu_supports_avx2, simd_impl_avx2 },
//   #endif
//   #if defined(__ARM_NEON)
//       { &cpu_supports_neon, simd_impl_neon },
//   #endif
//   }, scalar_impl);
//   return fn(args...);

namespace simd {

// If the environment variable OPTERYX_DISABLE_SIMD is set, the dispatcher will
// prefer the scalar fallback. This is useful for reproducing crashes caused by
// executing SIMD instructions on unsupported CPUs and for testing.
inline bool simd_disabled_by_env() {
    static std::once_flag once;
    static bool disabled = false;
    std::call_once(once, [](){
        const char* val = std::getenv("OPTERYX_DISABLE_SIMD");
        disabled = (val != nullptr && val[0] != '\0');
    });
    return disabled;
}

// NOTE: OPTERYX_FORCE_AVX2 environment flagging removed. We no longer prefer
// AVX2 over other supported SIMD implementations via an env var.

template <typename Fn>
Fn select_dispatch(std::atomic<Fn>& cache, std::initializer_list<std::pair<bool (*)(), Fn>> candidates, Fn fallback) {
    // Fast path: cached pointer
    Fn ptr = cache.load(std::memory_order_acquire);
    if (ptr) return ptr;

    // If user asked to disable SIMD at runtime, pick the fallback immediately.
    if (simd_disabled_by_env()) {
        cache.store(fallback, std::memory_order_release);
        return fallback;
    }

    // No explicit AVX2 forcing available; continue to normal selection
    // below which prefers the first supported candidate.

    for (auto &c : candidates) {
        auto check = c.first;
        Fn fn = c.second;
        if (check && check()) {
            cache.store(fn, std::memory_order_release);
            return fn;
        }
    }

    cache.store(fallback, std::memory_order_release);
    return fallback;
}

} // namespace simd
