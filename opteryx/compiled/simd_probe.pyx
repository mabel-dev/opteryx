# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

cdef extern from "simd_env.h":
    void opteryx_check_simd_env_or_abort()
    int opteryx_cpu_supports_avx2()
    int opteryx_cpu_supports_neon()

def check_env_or_abort():
    """Call into the native shim which will abort the process if
    OPTERYX_FAIL_IF_NOT_* is set and the CPU doesn't support the feature.
    """
    opteryx_check_simd_env_or_abort()

def cpu_supports_avx2() -> bool:
    return bool(opteryx_cpu_supports_avx2())

def cpu_supports_neon() -> bool:
    return bool(opteryx_cpu_supports_neon())



def cpu_architecture() -> list:
    architecture = []
    if cpu_supports_avx2():
        architecture.append("AVX2")
    if cpu_supports_neon():
        architecture.append("NEON")
    return architecture
