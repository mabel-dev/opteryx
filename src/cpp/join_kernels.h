#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/container/flat_hash_map.h"

#include "identity_hash.h"
#include "intbuffer.h"

using JoinVector = std::vector<std::int64_t>;
using JoinHashMap = absl::flat_hash_map<std::uint64_t, JoinVector, IdentityHash>;

void inner_join_probe(
    JoinHashMap* left_map,
    const std::int64_t* non_null_indices,
    std::size_t non_null_count,
    const std::uint64_t* row_hashes,
    std::size_t row_hash_count,
    CIntBuffer* left_out,
    CIntBuffer* right_out);
