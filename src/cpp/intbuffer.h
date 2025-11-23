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
    void append(unsigned long value);
    void append(unsigned long long value);
    void append(int64_t value1, int64_t value2);
    void append(const int64_t* values, size_t count);
    void append_optimized(int64_t value);
    void append_repeated(int64_t value, size_t count);

    void extend(const std::vector<int64_t>& values);
    void extend(const int64_t* values, size_t count);

    void reserve(size_t additional_capacity);
    void resize(size_t new_size);

    const int64_t* data() const noexcept;
    int64_t* mutable_data() noexcept;
    size_t size() const noexcept;
    size_t capacity() const noexcept;

    void shrink_to_fit();
    void clear() noexcept;

    template<typename InputIt>
    void extend(InputIt first, InputIt last);

private:
    std::vector<int64_t> buffer;
};

class CInt32Buffer {
public:
    explicit CInt32Buffer(size_t size_hint = 1024);

    void append(int32_t value);
    void extend(const std::vector<int32_t>& values);
    void extend(const int32_t* values, size_t count);

    void reserve(size_t additional_capacity);
    void resize(size_t new_size);

    const int32_t* data() const noexcept;
    int32_t* mutable_data() noexcept;
    size_t size() const noexcept;
    size_t capacity() const noexcept;

    void shrink_to_fit();
    void clear() noexcept;

private:
    std::vector<int32_t> buffer;
};