#include "join_kernels.h"

void inner_join_probe(
    JoinHashMap* left_map,
    const std::int64_t* non_null_indices,
    std::size_t non_null_count,
    const std::uint64_t* row_hashes,
    std::size_t row_hash_count,
    CIntBuffer* left_out,
    CIntBuffer* right_out) {
    if (left_map == nullptr || non_null_indices == nullptr || row_hashes == nullptr ||
        left_out == nullptr || right_out == nullptr) {
        return;
    }

    for (std::size_t i = 0; i < non_null_count; ++i) {
        const std::int64_t row_idx = non_null_indices[i];
        if (row_idx < 0) {
            continue;
        }

        const std::size_t hash_index = static_cast<std::size_t>(row_idx);
        if (hash_index >= row_hash_count) {
            continue;
        }

        const std::uint64_t hash_value = row_hashes[hash_index];
        auto it = left_map->find(hash_value);
        if (it == left_map->end()) {
            continue;
        }

        const JoinVector& matches = it->second;
        const std::size_t match_count = matches.size();
        if (match_count == 0) {
            continue;
        }

        left_out->extend(matches);
        right_out->append_repeated(row_idx, match_count);
    }
}
