#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

bool cpu_supports_avx2();
bool cpu_supports_neon();

#ifdef __cplusplus
}
#endif
