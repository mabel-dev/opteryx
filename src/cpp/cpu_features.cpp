#include "cpu_features.h"

#if defined(__linux__)
#include <string>
#include <fstream>
#include <sstream>
#include <iterator>
#endif

#if defined(__APPLE__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

#if defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif

#if defined(__GNUC__) || defined(__clang__)
// __builtin_cpu_supports is available on GCC/Clang on x86
#endif

#ifdef __x86_64__
#include <cpuid.h>
#endif

bool cpu_supports_avx2() {
#if defined(__x86_64__) || defined(__i386__)
#if (defined(__GNUC__) || defined(__clang__))
    // Prefer compiler-provided detection when available
#if defined(__has_builtin)
#if __has_builtin(__builtin_cpu_supports)
    if (__builtin_cpu_supports("avx2"))
        return true;
#endif
#endif
    // Fallback to __builtin_cpu_supports when GCC/Clang
    #ifdef __builtin_cpu_supports
    if (__builtin_cpu_supports("avx2"))
        return true;
    #endif

    // Try parsing /proc/cpuinfo on Linux
    #if defined(__linux__)
    std::ifstream f("/proc/cpuinfo");
    if (f) {
        std::string contents((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        if (contents.find("avx2") != std::string::npos)
            return true;
    }
    #endif
#endif
    return false;
#else
    return false;
#endif
}

bool cpu_supports_avx512() {
#if defined(__x86_64__) || defined(__i386__)
#if (defined(__GNUC__) || defined(__clang__))
    #ifdef __builtin_cpu_supports
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw"))
        return true;
    #endif

    #if defined(__linux__)
    std::ifstream f("/proc/cpuinfo");
    if (f) {
        std::string contents((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        if (contents.find("avx512f") != std::string::npos && contents.find("avx512bw") != std::string::npos)
            return true;
    }
    #endif
#endif
    return false;
#else
    return false;
#endif
}

bool cpu_supports_neon() {
#if defined(__arm__) || defined(__aarch64__)
    // On Linux, check HWCAP
    #if defined(__linux__)
    unsigned long hwcaps = getauxval(AT_HWCAP);
    #if defined(__aarch64__)
    // AArch64 HWCAP bit for NEON is typically HWCAP_ASIMD
    #ifdef HWCAP_ASIMD
    if (hwcaps & HWCAP_ASIMD)
        return true;
    #endif
    #else
    #ifdef HWCAP_NEON
    if (hwcaps & HWCAP_NEON)
        return true;
    #endif
    #endif
    #endif

    // Try /proc/cpuinfo flags
    #if defined(__linux__)
    std::ifstream f("/proc/cpuinfo");
    if (f) {
        std::string contents((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        if (contents.find("neon") != std::string::npos || contents.find("asimd") != std::string::npos)
            return true;
    }
    #endif

    // macOS / iOS: sysctl
    #if defined(__APPLE__)
    int has = 0;
    size_t sz = sizeof(has);
    if (sysctlbyname("hw.optional.neon", &has, &sz, nullptr, 0) == 0) {
        if (has)
            return true;
    }
    #endif

    return false;
#else
    return false;
#endif
}
