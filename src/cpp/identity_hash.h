#pragma once

#include <cstddef>
#include <cstdint>

struct IdentityHash {
    inline std::size_t operator()(std::uint64_t value) const {
        return static_cast<std::size_t>(value);
    }
};
