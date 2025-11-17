#pragma once

#include <cstddef>
#include <cstdint>

#include "absl/container/flat_hash_set.h"
#include "identity_hash.h"

namespace opteryx {

inline void flat_hash_set_insert_many(
    absl::flat_hash_set<std::uint64_t, IdentityHash>& target,
    const std::uint64_t* values,
    std::size_t length) {
    if (values == nullptr || length == 0) {
        return;
    }

    const std::uint64_t* begin = values;
    const std::uint64_t* end = values + length;
    target.insert(begin, end);
}

}  // namespace opteryx
