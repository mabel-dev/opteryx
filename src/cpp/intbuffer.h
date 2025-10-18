// intbuffer.h
#pragma once
#include <vector>
#include <cstdint>
#include <cstddef>

// Forward declarations for a small C++ helper used by Cython.
class CIntBuffer {
public:
    explicit CIntBuffer(size_t size_hint = 1024);

    void append(int64_t value);
    void append(int64_t value1, int64_t value2);
    void append(const int64_t* values, size_t count);
    void append_optimized(int64_t value);

    void extend(const std::vector<int64_t>& values);
    void extend(const int64_t* values, size_t count);

    void reserve(size_t additional_capacity);

    const int64_t* data() const noexcept;
    size_t size() const noexcept;
    size_t capacity() const noexcept;

    void shrink_to_fit();
    void clear() noexcept;

    template<typename InputIt>
    void extend(InputIt first, InputIt last);

private:
    std::vector<int64_t> buffer;
};